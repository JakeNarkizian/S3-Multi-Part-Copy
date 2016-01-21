from urlparse import urlparse
import boto
import uuid
import math
import itertools
import sys
import unittest


class copy_key_multipartTest(unittest.TestCase):
    def setUp(self):
        self.file_id = 'file_'+str(uuid.uuid1())
        self.upload_total_size = int(2**30 * 5.1)
        self.upload_chunk_size = int(2**20 * 5.1)
        self.upload_chunk_count = int(math.ceil(self.upload_total_size / float(self.upload_chunk_size)))

        self.s3 = boto.connect_s3()

        self.src = {}
        self.dst = {}

        self.src['name'] = 'mp_copy_test_src_bucket_'+str(uuid.uuid1())
        self.src['bucket'] = self.s3.create_bucket(self.src['name'])
        self.src['url'] = "s3://%s/%s"%(self.src['name'], self.file_id)
        self.src['filekey'] = self.src['bucket'].get_key(self.file_id)

        self.dst['name'] = 'mp_copy_test_dst_bucket_'+str(uuid.uuid1())
        self.dst['bucket'] = self.s3.create_bucket(self.dst['name'])
        self.dst['url'] = "s3://%s/%s"%(self.dst['name'], self.file_id)
        self.src['filekey'] = self.dst['bucket'].get_key(self.file_id)

        with open('/dev/zero', 'r') as f:
            upload = self.src['bucket'].initiate_multipart_upload(key_name=self.file_id)
            start = 0
            part_num = itertools.count()
            try:
                while start < self.upload_total_size:
                    end = min(start + self.upload_chunk_size, self.upload_total_size)
                    upload.upload_part_from_file(fp=f,
                                                 part_num=next(part_num) + 1,
                                                 size=end - start)
                    print "\ruploaded %d%%"%(int(math.ceil((float(start)* 100)/self.upload_total_size))),
                    sys.stdout.flush()
                    start = end
                assert start == self.upload_total_size
            except:
                upload.cancel_upload()
                raise
            else:
                upload.complete_upload()

    def teardown(self):
        for bkt in [self.src['bucket'], self.dst['bucket']]:
            for key in bkt.list():
                key.delete()
            self.s3.delete_bucket(bkt)

    def test_copy_key_multipart(self):
        copy_key_multipart(self.src['url'], self.dst['url'])

        try:
            assert self.src['filekey'].etag == self.dst['filekey'].etag
        except:
            print "etags do not match"
            raise
        else:
            print "test passed etags match"


def copy_key_multipart( src, dst ):
    """
    :param str src: a string containing a URL of the form s3://BUCKET_NAME/KEY_NAME pointing at
           the source object/key

    :param str dst: a string containing a URL of the form s3://BUCKET_NAME/KEY_NAME pointing at
           the destination object/key
    """
    # parse URLs
    src_url, dst_url = urlparse(src), urlparse(dst)
    assert src_url.scheme == dst_url.scheme == 's3'
    src_bkt_nme, dst_bkt_nme = src_url.netloc, dst_url.netloc
    src_key, dst_key = src_url.path[1:], dst_url.path[1:]

    # Connect to S3
    s3 = boto.connect_s3()
    src_bkt, dst_bkt = s3.get_bucket(src_bkt_nme), s3.get_bucket(dst_bkt_nme)

    # Multipart upload
    prt_size = 2**20 * 5 # 2^20 * 5 == 50MiB
    ttl_size = src_bkt.lookup(src_key).size
    assert prt_size < ttl_size
    mp = dst_bkt.initiate_multipart_upload(dst_key, encrypt_key=True, reduced_redundancy=True)

    pos, i = 0, 1
    while pos < ttl_size:
        lst = pos + (prt_size-1)
        if lst > ttl_size:
            lst = ttl_size - 1
        mp.copy_part_from_key(src_bkt_nme, src_key, i, pos, lst)
        pos += prt_size
        i += 1

    mp.complete_upload()

