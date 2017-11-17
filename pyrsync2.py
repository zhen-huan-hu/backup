"""
This is a pure Python implementation of the [rsync algorithm] [TM96].

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
    >>> patchstream(unpatched, save_to, delta)
"""

import hashlib

__all__ = [
    'weakchecksum',
    'rollingchecksum',
    'blockchecksums'
    'rsyncdelta',
    'patchstream',
    ]


def rsyncdelta(datastream, hashes, blocksize=4096, max_buffer=4096):
    """
    A binary patch generator when supplied with a readable stream for the
    up-to-date data and the weak and strong hashes from an unpatched target.
    The blocksize must be the same as the value used to generate the hashes.
    """
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
            # populate the window using weakchecksum instead of rolling
            # through every single byte which takes at least twice as long.
            window = bytearray(datastream.read(blocksize))
            if window:
                window_offset = 0
                checksum, a, b = weakchecksum(window)
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
            checksum, a, b = rollingchecksum(oldbyte, newbyte, a, b, blocksize)

        strongkey = hashlib.md5(window[window_offset:]).digest() if (
                checksum in hashdict) else None
        if checksum in hashdict and strongkey in hashdict[checksum]:
            match = True

            if current_block:
                yield bytes(current_block)
                current_block = bytearray()
            yield hashdict[checksum][strongkey]

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


def blockchecksums(instream, blocksize=4096):
    """
    A generator of the (weak hash (int), strong hash (bytes)) tuples
    for each block of the defined size for the given data stream.
    """
    read = instream.read(blocksize)
    while read:
        yield (weakchecksum(read)[0], hashlib.md5(read).digest())
        read = instream.read(blocksize)


def patchstream(instream, outstream, delta, blocksize=4096):
    """
    Patches instream using the supplied delta and write the resultantant
    data to outstream.
    """
    for element in delta:
        if isinstance(element, int) and blocksize:
            instream.seek(element * blocksize)
            element = instream.read(blocksize)
        outstream.write(element)


def rollingchecksum(old, new, a, b, blocksize=4096):
    """
    Generate a new weak checksum when supplied with
    the internal state of the checksum calculation for the previous window,
    the old byte, and the new byte.
    """
    a -= old - new
    b -= old * blocksize - a
    return (b << 16) | a, a, b


def weakchecksum(data):
    """
    Generate a weak checksum from an iterable set of bytes.
    """
    a = b = 0
    l = len(data)
    for i in range(l):
        a += data[i]
        b += (l - i) * data[i]
    return (b << 16) | a, a, b
