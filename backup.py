import argparse
import datetime
import io
import os
import re
import socket
import sys
import tarfile

import pyrsync2


def parse_input():
    parser = argparse.ArgumentParser(
            description='Python script to backup files in a TAR archive.')
    parser.add_argument(
            '-t', '--target', nargs=1, required=True,
            help='target directory')
    parser.add_argument(
            '-s', '--source', nargs='+', required=True,
            help='source directories or files')
    parser.add_argument(
            '-e', '--extract-mode', action='store_true',
            help='toggle extract mode')
    parser.add_argument(
            '-d', '--diff', nargs=1, default=[],
            help='source diff file')
    parser.add_argument(
            '-c', '--compress', action='store_true',
            help='compress the archive')
    parser.add_argument(
            '-r', '--rsync', action='store_true',
            help='create differential copies based on rsync algorithm')
    parser.add_argument(
            '-l', '--size-limit', dest='size', nargs=1, type=int, default=[0],
            help='exclude files larger than specified size, in bytes')
    parser.add_argument(
            '-f', '--file-type', dest='filetype', nargs='+', default=[],
            help='select file types to be archived')
    parser.add_argument(
            '-k', '--keep', nargs=1, type=int, default=[0],
            help='maximum archive iterations to keep')
    parser.add_argument(
            '-v', '--verbose', action='store_true',
            help='explain what is being done')

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit()
    return parser.parse_args()


def list_archives(tardir, tarpattern):
    try:
        # List files matching the pattern by their modification time
        tarfiles = [
                os.path.join(tardir, f) for f in os.listdir(tardir)
                if os.path.isfile(os.path.join(tardir, f)) and
                tarpattern.match(f)
                ]
        tarfiles.sort(key=os.path.getmtime)
        return tarfiles

    except FileNotFoundError:
        return []


def purge_archives(tardir, keep):
    if keep > 0:
        count = 0
        tarpattern = re.compile(
                '{0}-\d{{4}}-\d{{2}}-\d{{2}}-\d{{3,}}'.format(
                    socket.gethostname()))
        tarextpattern = re.compile('\.tar(\.lzma)?$')
        for tarpath in reversed(list_archives(tardir, tarpattern)):
            if count < keep:
                if re.search(tarextpattern, tarpath):
                    count += 1
            else:
                os.remove(tarpath)


def get_archive_name(tardir, compress):
    tarpattern = re.compile(
            '{0}-{1:%Y-%m-%d}-(\d{{3,}})'.format(
                socket.gethostname(), datetime.date.today()))
    tarfns = [os.path.split(f)[1] for f in list_archives(tardir, tarpattern)]
    taridx = int(tarpattern.match(tarfns[-1]).group(1)) + 1 if tarfns else 0
    tarext = 'tar.lzma' if compress else 'tar'
    return os.path.join(
            tardir, '{0}-{1:%Y-%m-%d}-{2:03d}.{3}'.format(
                socket.gethostname(), datetime.date.today(),
                taridx, tarext))


def archive_files(
        source, target, compress=False, rsync=False,
        filetype=None, limit=0, verbose=False):
    verboseprint = print if verbose else lambda *a, **k: None
    try:
        # Open TAR file for writing
        with tarfile.open(target, 'w:xz' if compress else 'w') as target_fid:
            for root in source:
                verboseprint('Adding {}'.format(root))
                target_fid.add(
                        root, filter=lambda fileinfo: (
                            None if fileinfo.isfile() and (
                                (filetype and os.path.splitext(
                                    fileinfo.name)[1] not in filetype) or
                                (0 < limit < fileinfo.size))
                            else fileinfo))

    except FileNotFoundError as not_found:
        if not_found.filename == target:
            tardir = os.path.dirname(target)
            verboseprint('Making directory {}'.format(tardir))
            os.makedirs(tardir)
            return archive_files(
                    source, target, compress, rsync,
                    filetype, limit, verbose)
        else:
            print(not_found.filename + ' not found')
            return False

    else:
        if rsync:
            tardir, tarfn = os.path.split(target)
            target_iteration = list_archives(
                    tardir,
                    # Each month is an iteration
                    re.compile(re.match('{0}-\d{{4}}-\d{{2}}'.format(
                        socket.gethostname()), tarfn).group(0)))
            if len(target_iteration) > 1:
                # Write diff file based on rsync algorithm
                with open(target_iteration[0], 'rb') as target_father_fid, \
                        open(target, 'rb') as target_fid, \
                        open(target + '.diff', 'wb') as diff_fid:
                    verboseprint('Making diff file')
                    hashes = pyrsync2.blockchecksums(target_father_fid)
                    delta = pyrsync2.rsyncdelta(target_fid, hashes)
                    for element in delta:
                        if isinstance(element, int):
                            diff_fid.write(b'\x00\x00')
                            diff_fid.write(element.to_bytes(8, byteorder='big'))
                        else:
                            diff_fid.write(
                                    len(element).to_bytes(2, byteorder='big'))
                            diff_fid.write(element)
                os.remove(target)
        return True


def extract_files(source, target, diff=None, verbose=False):
    verboseprint = print if verbose else lambda *a, **k: None
    try:
        if diff is not None:
            with open(source, 'rb') as source_fid, \
                    open(diff, 'rb') as diff_fid, \
                    io.BytesIO() as tmp_fid:
                byte = diff_fid.read(2)
                while byte:
                    if byte == b'\x00\x00':
                        offset = int.from_bytes(
                                diff_fid.read(8), byteorder='big')
                        source_fid.seek(offset * 4096)
                        tmp_fid.write(source_fid.read(4096))
                    else:
                        tmp_fid.write(
                                diff_fid.read(
                                    int.from_bytes(byte, byteorder='big')))
                    byte = diff_fid.read(2)
                tmp_fid.seek(0)
                with tarfile.open(mode='r|*', fileobj=tmp_fid) as tmp_tarfid:
                    tmp_tarfid.extractall(target)
        else:
            with tarfile.open(source, mode='r') as source_fid:
                source_fid.extractall(target)

    except FileNotFoundError as not_found:
        if not_found.filename == target:
            verboseprint('Making directory {}'.format(target))
            os.makedirs(target)
            return extract_files(source, target, diff, verbose)
        else:
            print(not_found.filename + ' not found')
            return False

    else:
        return True


def main():
    arg = parse_input()
    verboseprint = print if arg.verbose else lambda *a, **k: None
    if arg.extract_mode:
        rc = extract_files(
                arg.source[0], arg.target[0],
                arg.diff[0] if arg.diff else None,
                arg.verbose)
        if rc:
            verboseprint('Extracting completed')

    else:
        rc = archive_files(
                arg.source,
                get_archive_name(arg.target[0], arg.compress),
                arg.compress, arg.rsync,
                arg.filetype, arg.size[0],
                arg.verbose)
        if rc:
            verboseprint('Archiving completed')
            purge_archives(arg.target[0], arg.keep[0])


if __name__ == '__main__':
    main()
