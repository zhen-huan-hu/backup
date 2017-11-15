# backup

A Python script to backup files in a TAR archive with the following features:

    - Compression using the LZMA method
    - Differential backups based on the rsync algorithm
    - Rotation of the backup files
    - Choosing of specific file types or file size

This is a pure Python implementation and no additional external application is needed. Python 3.0+ is needed to run the script. The pyrsync2 module included in the package was originally written by Georgy Angelov and Isis Lovecruft and is licensed under the MIT License.

## Use

Basic syntax:

    backup.py -t TARGET -s SOURCE [SOURCE ...] [-e] [-d DIFF] [-c]
              [-r] [-l SIZE] [-f FILETYPE [FILETYPE ...]] [-k KEEP] [-v] [-h]

Required arguments:

    -t TARGET, --target TARGET
                          target directory
    -s SOURCE [SOURCE ...], --source SOURCE [SOURCE ...]
                          source directories or files
                          
Optional arguments:

    -e, --extract-mode    toggle extract mode
    -d DIFF, --diff DIFF  source diff file
    -c, --compress        compress the archive
    -r, --rsync           create differential copies based on rsync algorithm
    -l SIZE, --size-limit SIZE
                          exclude files larger than specified size, in bytes
    -f FILETYPE [FILETYPE ...], --file-type FILETYPE [FILETYPE ...]
                          select file types to be archived
    -k KEEP, --keep KEEP  maximum archive iterations to keep
    -v, --verbose         explain what is being done
    -h, --help            show help message and exit

The `-c, --compress`, `-r, --rsync`, `-l SIZE, --size-limit SIZE`, `-f FILETYPE [FILETYPE ...], --file-type FILETYPE [FILETYPE ...]`, and `-k KEEP, --keep KEEP` arguments work under the archiving mode. The `-e, --extract-mode` argument toggles the extract mode. The `-d DIFF, --diff DIFF` argument only works under the extract mode.

For archiving large amount of data such as the entire partition, it is recommended to use `-r, --rsync` without `-c, --compression`. On contrary, using `-c, --compression` without `-r, --rsync` is more desirable for archiving a small amount of files.

## License

This project is licensed under the GPL-3.0 License
