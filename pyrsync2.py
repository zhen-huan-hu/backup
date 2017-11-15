"""
This is a pure Python implementation of the [rsync algorithm] [TM96].

### Example Use Case: ###

    # On the system containing the file that needs to be patched
    >>> unpatched = open("unpatched.file", "rb")
    >>> hashes = blockchecksums(unpatched)

    # On the remote system after having received `hashes`
    >>> patchedfile = open("patched.file", "rb")
    >>> delta = rsyncdelta(patchedfile, hashes)

    # System with the unpatched file after receiving `delta`
    >>> unpatched.seek(0)
    >>> save_to = open("locally-patched.file", "wb")
    >>> patchstream(unpatched, save_to, delta)
"""

import hashlib

__all__ = [
    "rollingchecksum",
    "weakchecksum",
    "patchstream",
    "rsyncdelta",
    "blockchecksums"
]


def rsyncdelta(datastream, remotesignatures, blocksize=4096, max_buffer=4096):
    """
    Generates a binary patch when supplied with the weak and strong
    hashes from an unpatched target and a readable stream for the
    up-to-date data. The blocksize must be the same as the value
    used to generate remotesignatures.
    """

    remotesignatures = {
        weak: (index, strong) for index, (weak, strong)
        in enumerate(remotesignatures)
    }
    match = True
    matchblock = -1
    current_block = bytearray()

    while True:
        if match and datastream is not None:
            # Whenever there is a match or the loop is running for the first
            # time, populate the window using weakchecksum instead of rolling
            # through every single byte which takes at least twice as long.
            window = bytearray(datastream.read(blocksize))
            window_offset = 0
            checksum, a, b = weakchecksum(window)

        if (checksum in remotesignatures and
                remotesignatures[checksum][1] ==
                hashlib.md5(window[window_offset:]).digest()):

            matchblock = remotesignatures[checksum][0]

            match = True

            if len(current_block) > 0:
                yield bytes(current_block)

            yield matchblock
            current_block = bytearray()

            if datastream is None or datastream.closed:
                break
            continue

        else:
            # The weakchecksum (or the strong one) did not match
            match = False
            try:
                if datastream:
                    # Get the next byte and affix to the window
                    newbyte = ord(datastream.read(1))
                    window.append(newbyte)
            except TypeError:
                # No more data from the file; the window will slowly shrink.
                # newbyte needs to be zero from here on to keep the checksum
                # correct.
                newbyte = 0
                tailsize = datastream.tell() % blocksize
                datastream = None

            if datastream is None and len(window) - window_offset <= tailsize:
                # The likelihood that any blocks will match after this is
                # nearly nil so call it quits.

                # Flush the current block
                if len(current_block) > 0:
                    yield bytes(current_block)

                current_block = window[window_offset:]

                break

            # Yank off the extra byte and calculate the new window checksum
            oldbyte = window[window_offset]
            window_offset += 1
            checksum, a, b = rollingchecksum(oldbyte, newbyte, a, b, blocksize)

            if len(current_block) >= max_buffer:
                yield bytes(current_block)
                current_block = bytearray()

            # Add the old byte the file delta. This is data that was not found
            # inside of a matching block so it needs to be sent to the target.
            current_block.append(oldbyte)

    if len(current_block) > 0:
        yield bytes(current_block)


def blockchecksums(instream, blocksize=4096):
    """
    Generator of (weak hash (int), strong hash(bytes)) tuples
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


def rollingchecksum(removed, new, a, b, blocksize=4096):
    """
    Generates a new weak checksum when supplied with the internal state
    of the checksum calculation for the previous window, the removed
    byte, and the added byte.
    """
    a -= removed - new
    b -= removed * blocksize - a
    return (b << 16) | a, a, b


def weakchecksum(data):
    """
    Generates a weak checksum from an iterable set of bytes.
    """
    a = b = 0
    l = len(data)
    for i in range(l):
        a += data[i]
        b += (l - i) * data[i]

    return (b << 16) | a, a, b
