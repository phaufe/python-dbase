# python-dbase

Python module to handle dbase file operations (read, write, etc).

## Usage

### Create a dBase file and insert records

```py
fd = open( 'file.dbf', 'w+b' )

dbf = dbase.DBF.create( fd,
    (
        ( 'COLUNA1', PyDBF.types.Char,        8, 0 ),
        ( 'COLUNA2', PyDBF.types.Date,        8, 0 ),
    ),
    records = (
        ( 'test1', datetime.now(), ),
        ( 'test2', datetime(2001,2,3), ),
        ( 'test3', datetime(1901,12,13), ),
    ),
    n_records = 3,
)

fd.close()
```
