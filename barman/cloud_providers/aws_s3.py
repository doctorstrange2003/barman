# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2018-2023
#
# This file is part of Barman.
#
# Barman is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Barman is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Barman.  If not, see <http://www.gnu.org/licenses/>

import logging
import shutil
from io import RawIOBase

from barman.clients.cloud_compression import decompress_to_file
from barman.cloud import (
    CloudInterface,
    CloudProviderError,
    CloudSnapshotInterface,
    DecompressingStreamingIO,
    DEFAULT_DELIMITER,
    SnapshotMetadata,
    SnapshotsInfo,
)
from barman.exceptions import SnapshotBackupException


try:
    # Python 3.x
    from urllib.parse import urlencode, urlparse
except ImportError:
    # Python 2.x
    from urlparse import urlparse
    from urllib import urlencode

try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError, EndpointConnectionError
except ImportError:
    raise SystemExit("Missing required python module: boto3")


_logger = logging.getLogger(__name__)


class StreamingBodyIO(RawIOBase):
    """
    Wrap a boto StreamingBody in the IOBase API.
    """

    def __init__(self, body):
        self.body = body

    def readable(self):
        return True

    def read(self, n=-1):
        n = None if n < 0 else n
        return self.body.read(n)


class S3CloudInterface(CloudInterface):
    # S3 multipart upload limitations
    # http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html
    MAX_CHUNKS_PER_FILE = 10000
    MIN_CHUNK_SIZE = 5 << 20

    # S3 permit a maximum of 5TB per file
    # https://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html
    # This is a hard limit, while our upload procedure can go over the specified
    # MAX_ARCHIVE_SIZE - so we set a maximum of 1TB per file
    MAX_ARCHIVE_SIZE = 1 << 40

    MAX_DELETE_BATCH_SIZE = 1000

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove boto3 client reference from the state as it cannot be pickled
        # in Python >= 3.8 and multiprocessing will pickle the object when the
        # worker processes are created.
        # The worker processes create their own boto3 sessions so do not need
        # the boto3 session from the parent process.
        del state["s3"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __init__(
        self,
        url,
        encryption=None,
        jobs=2,
        profile_name=None,
        endpoint_url=None,
        tags=None,
        delete_batch_size=None,
        read_timeout=None,
    ):
        """
        Create a new S3 interface given the S3 destination url and the profile
        name

        :param str url: Full URL of the cloud destination/source
        :param str|None encryption: Encryption type string
        :param int jobs: How many sub-processes to use for asynchronous
          uploading, defaults to 2.
        :param str profile_name: Amazon auth profile identifier
        :param str endpoint_url: override default endpoint detection strategy
          with this one
        :param int|None delete_batch_size: the maximum number of objects to be
          deleted in a single request
        :param int|None read_timeout: the time in seconds until a timeout is
          raised when waiting to read from a connection
        """
        super(S3CloudInterface, self).__init__(
            url=url,
            jobs=jobs,
            tags=tags,
            delete_batch_size=delete_batch_size,
        )
        self.profile_name = profile_name
        self.encryption = encryption
        self.endpoint_url = endpoint_url
        self.read_timeout = read_timeout

        # Extract information from the destination URL
        parsed_url = urlparse(url)
        # If netloc is not present, the s3 url is badly formatted.
        if parsed_url.netloc == "" or parsed_url.scheme != "s3":
            raise ValueError("Invalid s3 URL address: %s" % url)
        self.bucket_name = parsed_url.netloc
        self.bucket_exists = None
        self.path = parsed_url.path.lstrip("/")

        # Build a session, so we can extract the correct resource
        self._reinit_session()

    def _reinit_session(self):
        """
        Create a new session
        """
        config_kwargs = {}
        if self.read_timeout is not None:
            config_kwargs["read_timeout"] = self.read_timeout
        config = Config(**config_kwargs)

        session = boto3.Session(profile_name=self.profile_name)
        self.s3 = session.resource("s3", endpoint_url=self.endpoint_url, config=config)

    @property
    def _extra_upload_args(self):
        """
        Return a dict containing ExtraArgs to be passed to certain boto3 calls

        Because some boto3 calls accept `ExtraArgs: {}` and others do not, we
        return a nested dict which can be expanded with `**` in the boto3 call.
        """
        additional_args = {}
        if self.encryption:
            additional_args["ServerSideEncryption"] = self.encryption
        return additional_args

    def test_connectivity(self):
        """
        Test AWS connectivity by trying to access a bucket
        """
        try:
            # We are not even interested in the existence of the bucket,
            # we just want to try if aws is reachable
            self.bucket_exists = self._check_bucket_existence()
            return True
        except EndpointConnectionError as exc:
            logging.error("Can't connect to cloud provider: %s", exc)
            return False

    def _check_bucket_existence(self):
        """
        Check cloud storage for the target bucket

        :return: True if the bucket exists, False otherwise
        :rtype: bool
        """
        try:
            # Search the bucket on s3
            self.s3.meta.client.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError as exc:
            # If a client error is thrown, then check the error code.
            # If code was 404, then the bucket does not exist
            error_code = exc.response["Error"]["Code"]
            if error_code == "404":
                return False
            # Otherwise there is nothing else to do than re-raise the original
            # exception
            raise

    def _create_bucket(self):
        """
        Create the bucket in cloud storage
        """
        # Get the current region from client.
        # Do not use session.region_name here because it may be None
        region = self.s3.meta.client.meta.region_name
        logging.info(
            "Bucket '%s' does not exist, creating it on region '%s'",
            self.bucket_name,
            region,
        )
        create_bucket_config = {
            "ACL": "private",
        }
        # The location constraint is required during bucket creation
        # for all regions outside of us-east-1. This constraint cannot
        # be specified in us-east-1; specifying it in this region
        # results in a failure, so we will only
        # add it if we are deploying outside of us-east-1.
        # See https://github.com/boto/boto3/issues/125
        if region != "us-east-1":
            create_bucket_config["CreateBucketConfiguration"] = {
                "LocationConstraint": region,
            }
        self.s3.Bucket(self.bucket_name).create(**create_bucket_config)

    def list_bucket(self, prefix="", delimiter=DEFAULT_DELIMITER):
        """
        List bucket content in a directory manner

        :param str prefix:
        :param str delimiter:
        :return: List of objects and dirs right under the prefix
        :rtype: List[str]
        """
        if prefix.startswith(delimiter):
            prefix = prefix.lstrip(delimiter)

        res = self.s3.meta.client.list_objects_v2(
            Bucket=self.bucket_name, Prefix=prefix, Delimiter=delimiter
        )

        # List "folders"
        keys = res.get("CommonPrefixes")
        if keys is not None:
            for k in keys:
                yield k.get("Prefix")

        # List "files"
        objects = res.get("Contents")
        if objects is not None:
            for o in objects:
                yield o.get("Key")

    def download_file(self, key, dest_path, decompress):
        """
        Download a file from S3

        :param str key: The S3 key to download
        :param str dest_path: Where to put the destination file
        :param str|None decompress: Compression scheme to use for decompression
        """
        # Open the remote file
        obj = self.s3.Object(self.bucket_name, key)
        remote_file = obj.get()["Body"]

        # Write the dest file in binary mode
        with open(dest_path, "wb") as dest_file:
            # If the file is not compressed, just copy its content
            if decompress is None:
                shutil.copyfileobj(remote_file, dest_file)
                return

            decompress_to_file(remote_file, dest_file, decompress)

    def remote_open(self, key, decompressor=None):
        """
        Open a remote S3 object and returns a readable stream

        :param str key: The key identifying the object to open
        :param barman.clients.cloud_compression.ChunkedCompressor decompressor:
          A ChunkedCompressor object which will be used to decompress chunks of bytes
          as they are read from the stream
        :return: A file-like object from which the stream can be read or None if
          the key does not exist
        """
        try:
            obj = self.s3.Object(self.bucket_name, key)
            resp = StreamingBodyIO(obj.get()["Body"])
            if decompressor:
                return DecompressingStreamingIO(resp, decompressor)
            else:
                return resp
        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            if error_code == "NoSuchKey":
                return None
            else:
                raise

    def upload_fileobj(self, fileobj, key, override_tags=None):
        """
        Synchronously upload the content of a file-like object to a cloud key

        :param fileobj IOBase: File-like object to upload
        :param str key: The key to identify the uploaded object
        :param List[tuple] override_tags: List of k,v tuples which should override any
          tags already defined in the cloud interface
        """
        extra_args = self._extra_upload_args.copy()
        tags = override_tags or self.tags
        if tags is not None:
            extra_args["Tagging"] = urlencode(tags)
        self.s3.meta.client.upload_fileobj(
            Fileobj=fileobj, Bucket=self.bucket_name, Key=key, ExtraArgs=extra_args
        )

    def create_multipart_upload(self, key):
        """
        Create a new multipart upload

        :param key: The key to use in the cloud service
        :return: The multipart upload handle
        :rtype: dict[str, str]
        """
        extra_args = self._extra_upload_args.copy()
        if self.tags is not None:
            extra_args["Tagging"] = urlencode(self.tags)
        return self.s3.meta.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=key, **extra_args
        )

    def _upload_part(self, upload_metadata, key, body, part_number):
        """
        Upload a part into this multipart upload

        :param dict upload_metadata: The multipart upload handle
        :param str key: The key to use in the cloud service
        :param object body: A stream-like object to upload
        :param int part_number: Part number, starting from 1
        :return: The part handle
        :rtype: dict[str, None|str]
        """
        part = self.s3.meta.client.upload_part(
            Body=body,
            Bucket=self.bucket_name,
            Key=key,
            UploadId=upload_metadata["UploadId"],
            PartNumber=part_number,
        )
        return {
            "PartNumber": part_number,
            "ETag": part["ETag"],
        }

    def _complete_multipart_upload(self, upload_metadata, key, parts):
        """
        Finish a certain multipart upload

        :param dict upload_metadata:  The multipart upload handle
        :param str key: The key to use in the cloud service
        :param parts: The list of parts composing the multipart upload
        """
        self.s3.meta.client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=key,
            UploadId=upload_metadata["UploadId"],
            MultipartUpload={"Parts": parts},
        )

    def _abort_multipart_upload(self, upload_metadata, key):
        """
        Abort a certain multipart upload

        :param dict upload_metadata:  The multipart upload handle
        :param str key: The key to use in the cloud service
        """
        self.s3.meta.client.abort_multipart_upload(
            Bucket=self.bucket_name, Key=key, UploadId=upload_metadata["UploadId"]
        )

    def _delete_objects_batch(self, paths):
        """
        Delete the objects at the specified paths

        :param List[str] paths:
        """
        super(S3CloudInterface, self)._delete_objects_batch(paths)

        resp = self.s3.meta.client.delete_objects(
            Bucket=self.bucket_name,
            Delete={
                "Objects": [{"Key": path} for path in paths],
                "Quiet": True,
            },
        )
        if "Errors" in resp:
            for error_dict in resp["Errors"]:
                logging.error(
                    'Deletion of object %s failed with error code: "%s", message: "%s"'
                    % (error_dict["Key"], error_dict["Code"], error_dict["Message"])
                )
            raise CloudProviderError()


class AwsCloudSnapshotInterface(CloudSnapshotInterface):
    """
    Implementation of CloudSnapshotInterface for EBS snapshots as implemented in AWS
    as documented at:

        https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-creating-snapshot.html
    """

    def __init__(self, profile_name=None, endpoint_url=None):
        """ """
        self.profile_name = profile_name
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(profile_name=self.profile_name)

    def take_snapshot(self, backup_info, disk_zone, disk_name):
        """
        Take a snapshot of an EBS volume in AWS.

        :param barman.infofile.LocalBackupInfo backup_info: Backup information.
        :param str disk_zone: The zone in which the disk resides.
        :param str disk_name: The name of the source disk for the snapshot.
        :rtype: str
        :return: The name used to reference the snapshot with AWS.
        """
        # TODO instead of a disk_zone and disk_name we just need a volume ID
        # so this should be abscracted somehowe
        # Also, why is this even part of the public interface given it's an
        # internal detail of take_snapshot_backup?
        ec2_client = self.session.client(
            "ec2", region_name=disk_zone, endpoint_url=self.endpoint_url
        )
        # TODO we should be more descriptive with the description
        snapshot_description = "%s-%s" % (
            disk_name,
            backup_info.backup_id.lower(),
        )
        _logger.info(
            "Taking snapshot '%s' of disk '%s'", snapshot_description, disk_name
        )
        resp = ec2_client.create_snapshot(
            Description=snapshot_description,
            VolumeId=disk_name,
        )

        # TODO Unlike both Gcp and Azure where we await each snapshot in serial, here
        # we defer because we must instantiate a waiter ourselves and can give it
        # multiple snapshot IDs
        # we are going to have to figure out the status ourselves.
        return resp["SnapshotId"]

    def take_snapshot_backup(self, backup_info, instance_name, zone, disks):
        """
        Take a snapshot backup for the named instance.

        Creates a snapshot for each named disk and saves the required metadata
        to backup_info.snapshots_info as an AwsSnapshotsInfo object.

        :param barman.infofile.LocalBackupInfo backup_info: Backup information.
        :param str instance_name: The name of the VM instance to which the disks
            to be backed up are attached.
        :param str zone: The zone in which the snapshot disks and instance reside.
        :param list[str] disks: A list containing the names of the source disks.
        """
        # TODO zone not needed here, only required for creating the client (which
        # is actually a region anyway) - it's not needed for identifying the snapshot
        # basically.
        ec2_client = self.session.client(
            "ec2", region_name=zone, endpoint_url=self.endpoint_url
        )
        resp = ec2_client.describe_instances(InstanceIds=[instance_name])
        instance_metadata = resp["Reservations"][0]
        disk_metadata = instance_metadata["Instances"][0]["BlockDeviceMappings"]
        snapshots = []
        # TODO disks are volume IDs here
        for volume_id in disks:
            attached_disks = [
                d for d in disk_metadata if d["Ebs"]["VolumeId"] == volume_id
            ]
            if len(attached_disks) == 0:
                raise SnapshotBackupException(
                    "Disk %s not attached to instance %s" % (volume_id, instance_name)
                )
            assert len(attached_disks) == 1

            snapshot_id = self.take_snapshot(backup_info, zone, volume_id)
            snapshots.append(
                AwsSnapshotMetadata(
                    device_name=attached_disks[0]["DeviceName"],
                    snapshot_id=snapshot_id,
                )
            )

        # Now await all the snapshots
        snapshot_ids = [snapshot.identifier for snapshot in snapshots]
        _logger.info("Waiting for snapshot '%s' completion", ", ".join(snapshot_ids))
        waiter = ec2_client.get_waiter("snapshot_completed")
        try:
            waiter.wait(Filters=[{"Name": "snapshot-id", "Values": snapshot_ids}])
        except ClientError as exc:
            raise CloudProviderError(
                "Snapshots failed with error code %s: %s"
                % (exc.response["Error"]["Code"], exc.response["Error"])
            )

        backup_info.snapshots_info = AwsSnapshotsInfo(snapshots=snapshots)

    def delete_snapshot(self, snapshot_name):
        """
        Delete the specified snapshot.

        :param str snapshot_name: The short name used to reference the snapshot within
            AWS.
        """
        # TODO region must be saved in the snapshot metadata since it is necessary on
        # AWS to connect to the right place
        region = "eu-west-2"
        ec2_client = self.session.client(
            "ec2", region_name=region, endpoint_url=self.endpoint_url
        )
        # TODO snapshot_name is actually a snapshot_id
        try:
            ec2_client.delete_snapshot(SnapshotId=snapshot_name)
        except ClientError as exc:
            raise CloudProviderError(
                "Deletion of snapshot %s failed with error code %s: %s"
                % (snapshot_name, exc.response["Error"]["Code"], exc.response["Error"])
            )
        import pdb

        pdb.set_trace()

    def delete_snapshot_backup(self, backup_info):
        """
        Delete all snapshots for the supplied backup.

        :param barman.infofile.LocalBackupInfo backup_info: Backup information.
        """
        for snapshot in backup_info.snapshots_info.snapshots:
            _logger.info(
                "Deleting snapshot '%s' for backup %s",
                snapshot.identifier,
                backup_info.backup_id,
            )
            self.delete_snapshot(snapshot.identifier)

    def get_attached_devices(self, instance_name, zone):
        """
        Returns the non-boot devices attached to instance_name in zone.

        :param str instance_name: The name of the VM instance to which the disks
            to be backed up are attached.
        :param str zone: The zone in which the snapshot disks and instance reside.
        :rtype: dict[str,str]
        :return: A dict where the key is the disk name and the value is the device
            path for that disk on the specified instance.
        """
        # TODO on AWS this function will include the boot device
        # not a technical problem but means we're now lying

        ec2_client = self.session.client(
            "ec2", region_name=zone, endpoint_url=self.endpoint_url
        )
        resp = ec2_client.describe_instances(InstanceIds=[instance_name])
        assert len(resp["Reservations"]) == 1
        instance_metadata = resp["Reservations"][0]
        attached_devices = {}
        # TODO can't just assume 0 here
        for device in instance_metadata["Instances"][0]["BlockDeviceMappings"]:
            volume_id = device["Ebs"]["VolumeId"]
            # TODO we'd need to do another lookup to get a friendly disk name, so
            # perhaps we just work with IDs for now
            device_name = device["DeviceName"]
            attached_devices[volume_id] = device_name
        return attached_devices

    def get_attached_snapshots(self, instance_name, zone):
        """
        Returns the snapshots which are sources for disks attached to instance.

        Queries the instance metadata to determine which disks are attached and
        then queries the disk metadata for each disk to determine whether it was
        cloned from a snapshot. If it was cloned then the snapshot is added to the
        dict which is returned once all attached devices have been checked.

        :param str instance_name: The name of the VM instance to which the disks
            to be backed up are attached.
        :param str zone: The zone in which the snapshot disks and instance reside.
        :rtype: dict[str,str]
        :return: A dict where the key is the snapshot name and the value is the
            device path for the source disk for that snapshot on the specified
            instance.
        """
        attached_devices = self.get_attached_devices(instance_name, zone)

        ec2_client = self.session.client(
            "ec2", region_name=zone, endpoint_url=self.endpoint_url
        )
        attached_snapshots = {}
        for volume_id, device_name in attached_devices.items():
            # TODO here we get the disk metadata for determine what the sources are
            resp = ec2_client.describe_volumes(VolumeIds=[volume_id])
            if len(resp["Volumes"][0]["SnapshotId"]) > 0:
                attached_snapshots[resp["Volumes"][0]["SnapshotId"]] = device_name
        return attached_snapshots

    def instance_exists(self, instance_name, zone):
        """
        Determine whether the named instance exists in the specified zone.

        :param str instance_name: The name of the VM instance to which the disks
            to be backed up are attached.
        :param str zone: The zone in which the snapshot disks and instance reside.
        :rtype: bool
        :return: True if the named instance exists in zone, False otherwise.
        """
        # TODO here we are accepting a region, not a zone
        # TODO the instance_name is actually an instance_id

        # TODO this should be cached and created on init
        ec2_client = self.session.client(
            "ec2", region_name=zone, endpoint_url=self.endpoint_url
        )
        try:
            ec2_client.describe_instances(InstanceIds=[instance_name])
        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            if error_code == "InvalidInstanceID.NotFound":
                return False
            else:
                raise
        return True


class AwsSnapshotMetadata(SnapshotMetadata):
    """
    Specialization of SnapshotMetadata for AWS EBS snapshots.

    Stores the device_name and snapshot_name in the provider-specific field.
    """

    _provider_fields = ("device_name", "snapshot_id")

    def __init__(
        self,
        mount_options=None,
        mount_point=None,
        device_name=None,
        snapshot_id=None,
    ):
        """
        Constructor saves additional metadata for AWS snapshots.

        :param str mount_options: The mount options used for the source disk at the
            time of the backup.
        :param str mount_point: The mount point of the source disk at the time of
            the backup.
        :param str device_name: The device name used in the AWS API.
        :param str snapshot_name: The snapshot name used in the AWS API.
        :param str project: The AWS project name.
        """
        super(AwsSnapshotMetadata, self).__init__(mount_options, mount_point)
        self.device_name = device_name
        self.snapshot_id = snapshot_id

    @property
    def identifier(self):
        """
        An identifier which can reference the snapshot via the cloud provider.

        :rtype: str
        :return: The snapshot ID.
        """
        return self.snapshot_id

    @property
    def device(self):
        """
        The device path to the source disk on the compute instance at the time the
        backup was taken.

        :rtype: str
        :return: The full path to the source disk device.
        """
        return self.device_name


class AwsSnapshotsInfo(SnapshotsInfo):
    """
    Represents the snapshots_info field for AWS EBS snapshots.
    """

    _provider_fields = ()
    _snapshot_metadata_cls = AwsSnapshotMetadata

    def __init__(self, snapshots=None):
        """
        Constructor saves the list of snapshots if it is provided.

        :param list[SnapshotMetadata] snapshots: A list of metadata objects for each
            snapshot.
        """
        super(AwsSnapshotsInfo, self).__init__(snapshots)
        self.provider = "aws"
