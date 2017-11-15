import argparse
import os, sys, socket, datetime, re
import tarfile
import pyrsync2

def parse_input():
    parser = argparse.ArgumentParser(description='Simple script to backup files in a TAR archive.')
    parser.add_argument('-t', '--target', nargs=1, required=True, help='target directory')
    parser.add_argument('-s', '--source', nargs='+', required=True, help='source directories or files')
    parser.add_argument('-e', '--extract-mode', action='store_true', help='toggle extract mode')
    parser.add_argument('-d', '--diff', nargs=1, help='source diff file', default=[])
    parser.add_argument('-c', '--compress', action='store_true', help='compress the archive')
    parser.add_argument('-l', '--size-limit', dest='size', nargs=1, type=int, help='exclude files larger than specified size, in bytes', default=[0])
    parser.add_argument('-f', '--file-type', dest='filetype', nargs='+', help='select file types to be archived', default=[])
    parser.add_argument('-k', '--keep', nargs=1, type=int, help='keep only the specified number of archives under the target directory', default=[0])
    parser.add_argument('-v', '--verbose', action='store_true', help='explain what is being done')

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit()
    return parser.parse_args()

def list_archives(tardir, tarpattern):
    try:
        # List all files matching the naming pattern by their modification dates
        tarfiles = [os.path.join(tardir, f) for f in os.listdir(tardir)
                if os.path.isfile(os.path.join(tardir, f)) and tarpattern.match(f)]
        tarfiles.sort(key=os.path.getmtime)
        return tarfiles

    except FileNotFoundError:
        return []

def purge_archives(tardir, keep):
    count = 0
    tarpattern = re.compile('{0}-\d{{4}}-\d{{2}}-\d{{2}}-\d{{3,}}'.format(socket.gethostname()))
    tarextpattern = re.compile('\.tar(\.lzma)$')
    for tarpath in reversed(list_archives(tardir, tarpattern)):
        if count < keep:
            if re.search(tarextpattern, tarpath):
                count += 1
        else:
            os.remove(tarpath)

def get_archive_name(tardir, compress):
    tarpattern = re.compile('{0}-{1:%Y-%m-%d}-(\d{{3,}})'.format(socket.gethostname(), datetime.date.today()))
    tarfilenames = [os.path.split(f)[1] for f in list_archives(tardir, tarpattern)]
    count = int(tarpattern.match(tarfilenames[-1]).group(1)) + 1 if len(tarfilenames) > 0 else 0
    target = os.path.join(tardir,
            '{0}-{1:%Y-%m-%d}-{2:03d}'.format(socket.gethostname(), datetime.date.today(), count)
            + ('.tar.lzma' if compress else '.tar'))
    return target

def archive_files(source, target, compress=False, filetype=None, limit=0, verbose=False):
    try:
        # Open TAR file for LZMA compressed writing
        with tarfile.open(target, 'w:xz' if compress else 'w') as target_fid:
            for root in source:
                if verbose:
                    print('Adding {}'.format(root))
                target_fid.add(root, filter=lambda fileinfo:
                        None if fileinfo.isfile() and ((filetype is not None and os.path.splitext(fileinfo.name)[1] not in filetype) or (limit > 0 and fileinfo.size > limit)) else fileinfo)

        # Delta encoding
        tardir, tarfilename = os.path.split(target)
        target_iteration = list_archives(tardir, re.compile(re.match('{0}-\d{{4}}-\d{{2}}'.format(socket.gethostname()), tarfilename).group(0)))
        if len(target_iteration) > 1:
            with open(target_iteration[0], 'rb') as target_father_fid, open(target, 'rb') as target_fid, open(target + '.diff', 'wb') as target_diff_fid:
                hashes = pyrsync2.blockchecksums(target_father_fid)
                delta = pyrsync2.rsyncdelta(target_fid, hashes)
                for element in delta:
                    if isinstance(element, int):
                        target_diff_fid.write(b'\x00\x00')
                        target_diff_fid.write(element.to_bytes(8, byteorder=sys.byteorder))
                    else:
                        target_diff_fid.write(len(element).to_bytes(2, byteorder=sys.byteorder))
                        target_diff_fid.write(element)
            os.remove(target)

        return True

    except FileNotFoundError as not_found:
        if not_found.filename == target:
            tardir = os.path.dirname(target)
            if verbose:
                print('Creating {}'.format(tardir))
            os.makedirs(tardir)
            archive_files(source, target, compress=compress, filetype=filetype, limit=limit, verbose=verbose)
        else:
            print(not_found.filename + ' not found')

    return False

def extract_files(source, target, diff=None, verbose=False):
    try:
        if diff is not None:
            with open(source, 'rb') as target_fid, open(diff, 'rb') as target_diff_fid, open(source + '.tmp', 'wb') as target_tmp_fid:
                byte = target_diff_fid.read(2)
                while byte:
                    if byte == b'\x00\x00':
                        offset = int.from_bytes(target_diff_fid.read(8), byteorder=sys.byteorder)
                        target_fid.seek(offset * 4096)
                        target_tmp_fid.write(target_fid.read(4096))
                    else:
                        target_tmp_fid.write(target_diff_fid.read(int.from_bytes(byte, byteorder=sys.byteorder)))
                    byte = target_diff_fid.read(2)

        with tarfile.open(source if diff is None else source + '.tmp') as target_fid:
            target_fid.extractall(target)

        if diff is not None:
            os.remove(source + '.tmp')

        return True

    except FileNotFoundError as not_found:
        if not_found.filename == target:
            if verbose:
                print('Creating {}'.format(target))
            os.makedirs(target)
            extract_files(source, target, verbose=verbose)
        else:
            print(not_found.filename + ' not found')

    return False

def main():
    arg = parse_input()
    if arg.extract_mode:
        rc = extract_files(arg.source[0], arg.target[0], (arg.diff[0] if len(arg.diff) > 0 else None), arg.verbose)
        if rc and arg.verbose:
            print('Extract completed')

    else:
        rc = archive_files(arg.source, get_archive_name(arg.target[0], arg.compress), arg.compress, (arg.filetype if len(arg.filetype) > 0 else None), arg.size[0], arg.verbose)
        if rc:
            if arg.verbose:
                print('Archive completed')
            purge_archives(arg.target[0], arg.keep[0])

if __name__ == '__main__':
    main()

