# enconding utf-8

import os
import struct
from datetime import datetime

from collections import namedtuple

class DbfFieldType(object):
    '''dBase field type class
    parameters: dbase binary value, dbase field description
    '''
    
    def __init__(self, dbf_type, name):
        self.dbf_type = dbf_type
        self.name = name
    
    def to_dbf(self, field_descriptor, value):
        '''converts to binary form of a dbase field'''
        # empty value
        if value is None:
            return ' ' * field_descriptor.field_length
        # char type
        elif self.dbf_type == 'C':
            # lenght validation
            if len( value ) > field_descriptor.field_length:
                raise ValueError( 'DBFError: len( value ) > field_descriptor.field_length. "%s": "%s"' % (field_descriptor.field_name, value) )
            # encoding conversion, or normalization
            value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
            return value.ljust( field_descriptor.field_length )
        # number type
        elif self.dbf_type == 'N':
            lenght = field_descriptor.field_length
            if value < 0: # negative number
                lenght += -1
            string = '{: >'+ str( field_descriptor.field_length ) +'.'+ str( field_descriptor.field_decimal_places ) + 'f}'
            return string.format( value )
        # date type
        elif self.dbf_type == 'D':
            return '{:04d}{:02d}{:02d}'.format( value.year, value.month, value.day )
        # other types, not implemented
        else:
            raise ValueError( 'DbfFieldType convert error' )

# a namespace to organize dbase field types
types = namedtuple( 'DbfTypes', [ 'Char', 'Date', 'Logical', 'Memo', 'Numerical' ] )(
    Char = DbfFieldType( 'C', 'Char' ),
    Date = DbfFieldType( 'D', 'Date' ),
    Logical = DbfFieldType( 'L', 'Logical' ),
    Memo = DbfFieldType( 'M', 'Memo' ),
    Numerical = DbfFieldType( 'N', 'Numerical' ),
)

# all dbase versions
DBF_FILE_TYPE = {
    0x02: 'FoxBASE',
    0x03: 'FoxBASE+/Dbase III plus, no memo',
    0x30: 'Visual FoxPro',
    0x31: 'Visual FoxPro, autoincrement enabled',
    0x32: 'Visual FoxPro with field type Varchar or Varbinary',
    0x43: 'dBASE IV SQL table files, no memo',
    0x63: 'dBASE IV SQL system files, no memo',
    0x83: 'FoxBASE+/dBASE III PLUS, with memo',
    0x8B: 'dBASE IV with memo',
    0xCB: 'dBASE IV SQL table files, with memo',
    0xF5: 'FoxPro 2.x (or earlier) with memo',
    0xE5: 'HiPer-Six format with SMT memo file',
    0xFB: 'FoxBASE',
}


class DbfHeaderLastUpdate():
    '''class to represent dbase header last update record'''
    
    def __init__(self, datetime):
        self.datetime = datetime
    
    def to_tuple(self):
        '''converts internal datetime object to a tuple in the correct order'''
        return (
            self.datetime.year - 2000,
            self.datetime.month,
            self.datetime.day,
        )

class DbfHeader(object):
    '''class to read and write dbase file header'''
    
    STRUCT = '<4BI2H20x' # struct for DBASE III only
    HEADER_TERMINATOR = chr( 0x0D )
    
    def __init__(self, version, last_update, n_records, field_descriptors):
        self.version = version
        self.last_update = last_update
        self.n_records = n_records
        self.header_size = 32 + ( 32 * len( field_descriptors ) ) + 1
        self.data_record_size = sum( x.field_length for x in field_descriptors ) + 1
        self.field_descriptors = field_descriptors
    
    def to_binary(self):
        '''converts header data to binary form'''
        last_update_year, last_update_month, last_update_day = self.last_update.to_tuple()
        header_01 = struct.pack( self.STRUCT,
            self.version,
            last_update_year,
            last_update_month,
            last_update_day,
            self.n_records,
            self.header_size,
            self.data_record_size,
        )
        header_02 = ''.join( x.to_binary() for x in self.field_descriptors )
        header_03 = self.HEADER_TERMINATOR
        return header_01 + header_02 + header_03

class DbfHeaderFieldDescriptor(object):
    '''class to read and write dbase field descriptors'''
    
    STRUCT = '<11scI2B14x'
    
    def __init__(self, field_data_address, field_name, field_type, field_length, field_decimal_places):
        self.field_data_address = field_data_address
        self.field_name = field_name
        self.field_type = field_type
        self.field_length = field_length
        self.field_decimal_places = field_decimal_places
    
    def to_binary(self):
        '''converts field descriptor data to binary form'''
        return struct.pack( self.STRUCT,
            self.field_name.ljust( 11, chr( 0x00 ) ),
            self.field_type.dbf_type,
            self.field_data_address,
            self.field_length,
            self.field_decimal_places,
        )

# DBF file header

class DBF(object):
    '''main class, represents a DBF file'''
    
    DFB_FILE_TERMINATOR = chr( 0x1A )
    
    @staticmethod
    def create( fd, fields, records = None, n_records = 0 ):
        '''Static method to create (or overwrite) a new DBF file.
        
        'records' can be a generator if 'n_records' is informed 
        '''
        field_descriptors = []
        field_data_address = 1
        for field_name, field_type, field_length, field_decimal_places in fields:
            field_descriptors.append(
                DbfHeaderFieldDescriptor(
                    field_data_address,
                    field_name,
                    field_type,
                    field_length,
                    field_decimal_places
                )
            )
            field_data_address += field_length
        
        if records and not n_records:
            n_records = len( records )
        
        header = DbfHeader( 3, DbfHeaderLastUpdate( datetime.now() ), n_records, field_descriptors )
        
        fd.write( header.to_binary() )
        dbf_obj = DBF( fd, header = header )
        
        if records:
            for record in records:
                dbf_obj._write_record( record )
        
        dbf_obj._write_terminator()
        
        return dbf_obj
    
    def __init__(self, fd, header=None):
        self._fd = fd
        self.header = header
        
        if not self.header:
            fd.seek( 0 )
            #self.header = DbfHeader( fd.read() )
    
    def _write_record(self, record_tuple):
        '''writes a single record, assumes that the file pointer is in the right place'''
        self._fd.write(' ') # FLAG DELETE ou nao
        for value, field_descriptor in zip( record_tuple, self.header.field_descriptors ):
            self._fd.write( field_descriptor.field_type.to_dbf( field_descriptor, value ) )
    
    def _write_terminator(self):
        '''writes dbase file terminator symbol, assumes that the file pointer is in the right place'''
        self._fd.write( self.DFB_FILE_TERMINATOR )
    
    def _go_to_end_of_file(self, offset = 0):
        '''changes file pointer to the end of file'''
        self._fd.seek( offset, os.SEEK_END )
    
    def _find_and_remove_terminator(self):
        '''goes to end of file and removes the 'dbase file terminator symbol' if it exists'''
        self._go_to_end_of_file( -1 )
        last_char = self._fd.read( 1 )
        if last_char == self.DFB_FILE_TERMINATOR:
            self._go_to_end_of_file( -1 )
            self._fd.truncate( self._fd.tell() )
    
    def _read_n_records(self):
        '''reads the value of dbase file 'number of records' from the header'''
        fd_pos = self._fd.tell()
        self._fd.seek( 4 )
        value = ord( self._fd.read( 1 ) )
        self._fd.seek( fd_pos )
        return value
    
    def _update_n_records(self, value = 1):
        '''updates the value of dbase file 'number of records' in the header'''
        fd_pos = self._fd.tell()
        self._fd.seek( 4 )
        self._fd.write( chr( value + self._read_n_records() ) )
        self._fd.seek( fd_pos )
    
    def append(self, record_tuple):
        '''appends a record in the end of the file, dealing with the dbase terminator field'''
        self._find_and_remove_terminator()
        self._write_record( record_tuple )
        self._update_n_records( 1 )
        self._write_terminator()
