''' This is a pure Python implementation of the [rsync algorithm] [TM96].

### Example Use Case: ###

    # On the system containing the file that needs to be patched
    >>> unpatched = open('unpatched.file', 'rb')
    >>> hashes = blockchecksums(unpatched)

    # On the remote system after having received `hashes`
    >>> patchedfile = open('patched.file', 'rb')
    >>> delta = rsyncdelta(patchedfile, hashes)

    # System with the unpatched file after receiving `delta`
    >>> unpatched.seek(0)
    >>> save_to = open('locally-patched.file', 'wb')
    >>> patch_stream(unpatched, save_to, delta)
'''

import hashlib

__all__ = [
    'checksum', 'rolling_checksum',
    'blockchecksums', 'rsyncdelta',
    'patch_stream',
    ]


def rsyncdelta(datastream, hashes, blocksize=4096, max_buffer=4096):
    ''' Return an iterator of binary patches when supplied with a readable
        stream of the up-to-date data and a list of hash pair tuples from an
        unpatched target.
    '''
    hashdict = {}
    for index, (weak, strong) in enumerate(hashes):
        if weak not in hashdict:
            hashdict[weak] = {}
        hashdict[weak][strong] = index
        
    match = True
    current_block = bytearray()

    while True:
        if match:
            # Whenever there is a match or
            # the loop is running for the first time,
            # populate the window using checksum instead of rolling
            # through every single byte which takes at least twice as long.
            window = bytearray(datastream.read(blocksize))
            if window:
                window_offset = 0
                weakkey, a, b = checksum(window)
            else:
                break
        else:
            # Roll one byte forward if not already at the EOF
            if datastream is not None:
                newbytearray = bytearray(datastream.read(1))
                if newbytearray:
                    newbyte = newbytearray[0]
                    window.append(newbyte)
                else:
                    # EOF; the window will slowly shrink.
                    # newbyte needs to be zero from here on to keep
                    # the checksum correct.
                    newbyte = 0
                    tailsize = datastream.tell() % blocksize
                    datastream = None

            # Add the old byte the file delta. This is data that was not found
            # inside of a matching block so it needs to be sent to the target.
            oldbyte = window[window_offset]
            current_block.append(oldbyte)
            window_offset += 1
            # Yank off the extra byte and calculate the new window checksum
            weakkey, a, b = rolling_checksum(oldbyte, newbyte, a, b, blocksize)

        strongkey = hashlib.md5(window[window_offset:]).digest() if (
                weakkey in hashdict) else None
        if weakkey in hashdict and strongkey in hashdict[weakkey]:
            match = True

            if current_block:
                yield bytes(current_block)
                current_block = bytearray()
            yield hashdict[weakkey][strongkey]

            if datastream is None:
                break

        else:
            match = False

            if len(current_block) == max_buffer:
                yield bytes(current_block)
                current_block = bytearray()

            if datastream is None and len(window) - window_offset <= tailsize:
                # The likelihood that any blocks will match after this is
                # nearly nil so flush the current block and call it quits.
                if current_block:
                    yield bytes(current_block)
                    current_block = bytearray()
                yield bytes(window[window_offset:])
                break


def blockchecksums(in_stream, blocksize=4096):
    ''' Return an iterator of the (weak hash (int), strong hash (bytes)) tuples
        for each block of the given data stream.
    '''
    read = in_stream.read(blocksize)
    while read:
        yield (checksum(read)[0], hashlib.md5(read).digest())
        read = in_stream.read(blocksize)


def patch_stream(in_stream, out_stream, delta, blocksize=4096):
    ''' Patch the in-stream data based on the binary patch delta and write the
        resultantant data to the out-stream.
    '''
    for element in delta:
        if isinstance(element, int):
            in_stream.seek(element * blocksize)
            element = in_stream.read(blocksize)
        out_stream.write(element)


def rolling_checksum(old, new, a, b, blocksize=4096):
    ''' Return a new weak checksum when supplied with the internal state of the
        checksum calculation from the previous window, the removed old byte,
        and the added new byte.
    '''
    a -= old - new
    b -= old * blocksize - a
    return (b << 16) | a, a, b


def checksum(block):
    ''' Return a weak checksum from an iterable set of bytes. '''
    a = b = 0
    blocksize = len(block)
    for i in range(blocksize):
        a += block[i]
        b += (blocksize - i) * block[i]
    return (b << 16) | a, a, b
