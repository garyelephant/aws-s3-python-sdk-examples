"""
Yet another s3 python sdk example.
based on boto 2.27.0
"""

import time
import os
import urllib

import boto.s3.connection
import boto.s3.key

def test():
    print '--- running AWS s3 examples ---'
    c = boto.s3.connection.S3Connection('<YOUR_AWS_ACCESS_KEY>', '<YOUR_AWS_SECRET_KEY>')

    print 'original bucket number:', len(c.get_all_buckets())
    
    bucket_name = 'yet.another.s3.example.code'
    print 'creating a bucket:', bucket_name
    try:
        bucket = c.create_bucket(bucket_name)
    except boto.exception.S3CreateError  as e:
        print ' ' * 4, 'error occured:'
        print ' ' * 8, 'http status code:', e.status
        print ' ' * 8, 'reason:', e.reason
        print ' ' * 8, 'body:', e.body
        return

    test_bucket_name = 'no.existence.yet.another.s3.example.code'
    print 'if you just want to know whether the bucket(\'%s\') exists or not' % (test_bucket_name,), \
        'and don\'t want to get this bucket'
    try:
        test_bucket = c.head_bucket(test_bucket_name)
    except boto.exception.S3ResponseError as e:
        if e.status == 403 and e.reason == 'Forbidden':
            print ' ' * 4, 'the bucket(\'%s\') exists but you don\'t have the permission.' % (test_bucket_name,)
        elif e.status == 404 and e.reason == 'Not Found':
            print ' ' * 4, 'the bucket(\'%s\') doesn\'t exist.' % (test_bucket_name,)

    print 'or use lookup() instead of head_bucket() to do the same thing.', \
        'it will return None if the bucket does not exist instead of throwing an exception.'
    test_bucket = c.lookup(test_bucket_name)
    if test_bucket is None:
        print ' ' * 4, 'the bucket(\'%s\') doesn\'t exist.' % (test_bucket_name,)

    print 'now you can get the bucket(\'%s\')' % (bucket_name,)
    bucket = c.get_bucket(bucket_name)

    print 'add some objects to bucket ', bucket_name
    keys = ['sample.txt', 'notes/2006/January/sample.txt', 'notes/2006/February/sample2.txt',\
           'notes/2006/February/sample3.txt', 'notes/2006/February/sample4.txt', 'notes/2006/sample5.txt']
    print 'these key names are:'
    for name in keys:
        print ' ' * 4, name
        key = bucket.new_key(name)
        s = 'This is the content of %s ' % (name,)
        key.set_contents_from_string(s)

    #print 'You have %d objects in bucket %s' % ()    
    
    print 'list all objects added into \'%s\' bucket' % (bucket_name,)
    objs = bucket.list()
    for key in objs:
        print ' ' * 4, key.name

    p = 'notes/2006/'
    print 'list objects start with \'%s\'' % (p,)
    objs = bucket.list(prefix = p)
    for key in objs:
        print ' ' * 4, key.name

    print 'list objects or key prefixs like \'%s/*\', something like what\'s in the top of \'%s\' folder ?' % (p, p,)
    objs = bucket.list(prefix = p, delimiter = '/')
    for key in objs:
        print ' ' * 4, key.name

    keys_per_page = 4
    print 'manually handle the results paging from s3,', ' number of keys per page:', keys_per_page
    print ' ' * 4, 'get page 1'
    objs = bucket.get_all_keys(max_keys = keys_per_page)
    for key in objs:
        print ' ' * 8, key.name

    print ' ' * 4, 'get page 2'
    last_key_name = objs[-1].name   #last key of last page is the marker to retrive next page.
    objs = bucket.get_all_keys(max_keys = keys_per_page, marker = last_key_name)
    for key in objs:
        print ' ' * 8, key.name
    """
    get_all_keys() a lower-level method for listing contents of a bucket.
    This closely models the actual S3 API and requires you to manually handle the paging of results. 
    For a higher-level method that handles the details of paging for you, you can use the list() method.
    """

    print 'you must delete all objects in the bucket \'%s\' before delete this bucket' % (bucket_name, )
    print ' ' * 4, 'you can delete objects one by one'
    bucket.delete_key(keys[0])
    print ' ' * 4, 'or you can delete multiple objects using a single HTTP request with delete_keys().'
    bucket.delete_keys(keys[1:])

    #TODO print 'after previous deletion, we now have %d objects in bucket(\'%s\')' % (len(bucket.list()), bucket_name,)
    print 'now you can delete the bucket \'%s\'' % (bucket_name,)
    c.delete_bucket(bucket)

    #references:
    #  [1] http://docs.pythonboto.org/
    #  [2] amazon s3 api references

if __name__ == '__main__':
    test()
