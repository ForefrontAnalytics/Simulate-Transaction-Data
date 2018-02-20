"""
Helper functions to read lines from an AWS StreamingBody object.

At time of writing, botocore.response only supports reading bytes from
a StreamingBody object, with no way to read lines.

This script seek to emulate the behaviour of a .readline() method,
with the alteration that it will re-read from the same object in a cycle.
"""


def _get_object(s3_resource, bucket_name, object_key):
    """Retrieve the S3 object body"""
    obj = s3_resource.Object(bucket_name=bucket_name, key=object_key)
    response = obj.get()['Body']
    return response
    

def readlines(s3_resource, bucket_name, object_key):
    """
    Provide a generator to read a single line at a time from an S3 object.
    """
    streamingbody = _get_object(s3_resource, bucket_name, object_key)
    
    # This will store left over bytes from the previous reading of a chunk
    remnant = None
    
    while True:
        # Read a 1MB chunk of the stream, decode to UTF-8 and split into lines
        chunk = streamingbody.read(1000000)

        # If have finished reading stream will get empty bytestring, so start again
        if chunk == b'':
            obj = s3.Object(bucket_name=bucket_name, key=transactions)
            response = obj.get()['Body']
        
        raw_lines = chunk.decode('utf-8').splitlines()

        # If first character of chunk was a newline, discard
        if len(raw_lines[0]) == 0:
            raw_lines.pop(0)

        # If left over from previous chunk, prepend it to the first line here
        if remnant:
            raw_lines[0] = remnant + raw_lines[0]

        # Check if last entry
        last_line = raw_lines[-1]
        last_cols = last_line.split(',')
        # If don't have all 4 cols, or 4th col is just the comma, pull it off to prepend to next chunk
        if len(last_cols) != 4 or len(last_cols[3]) == 0:
            remnant = raw_lines.pop()
        else:
            remnant = None

        # Iterate through lines, split and convert to ints
        for line in raw_lines:
            int_line = [col for col in line.split(',')]
            yield int_line