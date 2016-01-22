from urlparse import urlparse
import boto
import uuid
import math
import itertools
import sys
import unittest
import io


chunk_size = int(2**20 * 5)
test_upload_size = int(2**20 * 5.3)

class copy_key_multipartTest(unittest.TestCase):
    def setUp(self):
        self.file_id = 'file_'+str(uuid.uuid1())
        self.upload_total_size = test_upload_size
        self.upload_chunk_size = chunk_size
        self.upload_chunk_count = int(math.ceil(self.upload_total_size / float(self.upload_chunk_size)))

        self.s3 = boto.connect_s3()

        self.src = {}
        self.dst = {}

        self.src['name'] = 'mp_copy_test_src_bucket_'+str(uuid.uuid1())
        self.src['bucket'] = self.s3.create_bucket(self.src['name'])
        self.src['url'] = "s3://%s/%s"%(self.src['name'], self.file_id)

        self.dst['name'] = 'mp_copy_test_dst_bucket_'+str(uuid.uuid1())
        self.dst['bucket'] = self.s3.create_bucket(self.dst['name'])
        self.dst['url'] = "s3://%s/%s"%(self.dst['name'], self.file_id)


        with open('/dev/urandom', 'r') as f:
            upload = self.src['bucket'].initiate_multipart_upload(key_name=self.file_id)
            start = 0
            part_num = itertools.count()
            try:
                while start < self.upload_total_size:
                    end = min(start + self.upload_chunk_size, self.upload_total_size)
                    b = io.BytesIO(f.read(self.upload_chunk_size))

                    upload.upload_part_from_file(fp=b,
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
            self.src['filekey'] = self.src['bucket'].get_key(self.file_id)
            self.dst['filekey'] = self.dst['bucket'].get_key(self.file_id)

            print self.src['filekey']
            print self.dst['filekey']

            assert self.src['bucket'].get_key(self.file_id).etag ==  self.dst['bucket'].get_key(self.file_id).etag
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
    src_bkt = s3.get_bucket(src_bkt_nme)
    dst_bkt = s3.get_bucket(dst_bkt_nme)

    # Multipart copy
    prt_size = chunk_size 
    ttl_size = src_bkt.lookup(src_key).size
    assert prt_size < ttl_size
    mp = dst_bkt.initiate_multipart_upload(dst_key)

    try:
        pos = 0
        part_num = itertools.count()
        while pos < ttl_size:
            end = min(pos + prt_size - 1, ttl_size-1) # bytes are indexed form zero
            mp.copy_part_from_key(src_bkt_nme,
                                  src_key,
                                  next(part_num)+1,
                                  pos,
                                  end)
            pos += prt_size
    except:
        mp.cancel_upload()
    else:
        mp.complete_upload()
