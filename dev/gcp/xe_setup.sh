touch /tmp/xe_test
mkfs.xfs /dev/sdb
mkdir /mnt/xenon/
mount /dev/sdb /mnt/xenon/
chmod -R 0777 /mnt/xenon
