#define DB_VER 0xDB000001L					// DB version
#define DB_MAXREC 65000						// DB Maximum Records allowed
#define DB_KEYLEN 128						// DB Maximum Key Length
#define DB_MAXRLN 4096						// DB Maximum Record Length

int db_open(char *file);					// open database
int db_create(char *file,long rln);			// create a new database
void db_close(int hnd);						// close opened database
int db_read(int hnd,char *key,void *data);	// read record from database
int db_next(int hnd,char *key,void *data);	// sequential read from database
int db_write(int hnd,char *key,void *data);	// write record to database
int db_delete(int hnd,char *key);			// delete record from database
