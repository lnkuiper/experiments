# GNU Sort
How to sort a CSV with GNU sort.

The basic command is as follows:
```
sort -t <DELIMITER> -k<COLNUM>[,<COLNUM>[n][r]] <FILENAME>
```

As many columns as needed can be specified with multiple ordered `-k` parameters. Everything between `[]` is optional. Sort order is lexicographical (default), but can be made numerical using `n`. Sort order can be reversed using `r`. To measure the total time, create a script that sorts and redirects the output to `/dev/null`, and call the script using `time ./my-script-sh`.
