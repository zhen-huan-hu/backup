import argparse
import os, sys, socket, datetime, re
import tarfile

def parse_input():
    parser = argparse.ArgumentParser(description='Simple script to backup files in a TAR archive.')
    parser.add_argument('-t', '--target', nargs=1, required=True, help='target directory')
    parser.add_argument('-s', '--source', nargs='+', required=True, help='source directories or files')
    parser.add_argument('-e', '--extract-mode', action='store_true', help='toggle extract mode')
    parser.add_argument('-c', '--compress', action='store_true', help='compress the archive')
    parser.add_argument('-l', '--size-limit', dest='size', nargs=1, type=int, help='exclude files larger than specified size, in bytes', default=[0])
    parser.add_argument('-f', '--file-type', dest='filetype', nargs='+', help='select file types to be archived', default=[])
    parser.add_argument('-k', '--keep', nargs=1, type=int, help='keep only the specified number of archives under the target directory', default=[0])
    parser.add_argument('-v', '--verbose', action='store_true', help='explain what is being done')

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit()
    return parser.parse_args()

def list_archives_sorted(tardir):
    try:
        # List and sort all files that match the archive naming pattern
        tarpattern = re.compile('{0}-\d{{4}}-\d{{2}}-\d{{2}}-\d{{3,}}'.format(socket.gethostname()))
        tarfiles = [os.path.join(tardir, f) for f in os.listdir(tardir)
                if os.path.isfile(os.path.join(tardir, f)) and tarpattern.match(f)]
        tarfiles.sort(key=os.path.getmtime)
        return tarfiles

    except FileNotFoundError:
        return []

def purge_archives(tardir, keep):
    tarfiles = list_archives_sorted(tardir)

    if keep > 0 and len(tarfiles) > keep:
        for f in tarfiles[:-keep]:
            os.remove(f)

def get_archive_name(tardir, compress):
    count = 0
    tardir = os.path.normpath(tardir)
    tarpattern = re.compile('{0}-{1:%Y-%m-%d}-(\d{{3,}})'.format(socket.gethostname(), datetime.date.today()))
    tarfilenames = [os.path.split(f)[1] for f in list_archives_sorted(tardir)]
    tarfilenames = [fn for fn in tarfilenames if tarpattern.match(fn)]
    if len(tarfilenames) > 0:
        count = int(tarpattern.match(tarfilenames[-1]).group(1)) + 1
    target = tardir + os.sep + '{0}-{1:%Y-%m-%d}-{2:03d}'.format(socket.gethostname(), datetime.date.today(), count) + ('.tar.lzma' if compress else '.tar')
    return target

def archive_files(source, target, compress=False, filetype=[], limit=0, verbose=False):
    try:
        # Open TAR file for LZMA compressed writing
        with tarfile.open(target, 'w:xz' if compress else 'w') as target_fid:
            for root in source:
                if verbose:
                    print('Adding {}'.format(root))
                target_fid.add(root, filter=lambda fileinfo:
                        None if fileinfo.isfile() and ((len(filetype) > 0 and os.path.splitext(fileinfo.name)[1] not in filetype) or (limit > 0 and fileinfo.size > limit)) else fileinfo)
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

def extract_files(source, target, verbose=False):
    try:
        with tarfile.open(source) as target_fid:
            target_fid.extractall(target)
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
        rc = extract_files(arg.source[0], arg.target[0], arg.verbose)
        if rc and arg.verbose:
            print('Extract completed')

    else:
        rc = archive_files(arg.source, get_archive_name(arg.target[0], arg.compress), arg.compress, arg.filetype, arg.size[0], arg.verbose)
        if rc:
            if arg.verbose:
                print('Archive completed')
            purge_archives(arg.target[0], arg.keep[0])

if __name__ == '__main__':
    main()

