CREATE TABLE tree (
	id BIGINTEGE PRIMARY KEY, 
	parent_id INTEGER, 
	name BIGINTEGER NOT NULL, 
	inode BIGINTEGER NOT NULL, 
	UNIQUE (parent_id, name)
);

CREATE TABLE strings (
	id INTEGER PRIMARY KEY, 
	value TEXT NOT NULL UNIQUE
);
	
CREATE TABLE inodes (
	inode BIGINTEGER PRIMARY KEY,
	nlinks INTEGER NOT NULL, 
	mode INTEGER NOT NULL, 
	uid INTEGER,
	gid INTEGER, 
	rdev INTEGER, 
	size INTEGER, 
	atime INTEGER, 
	mtime INTEGER, 
	ctime INTEGER
);

CREATE TABLE links (
	inode BIGINTEGER UNIQUE
	target BLOB NOT NULL
);

CREATE TABLE hashes (
	id BIGINTEGER PRIMARY KEY, 
	hash BLOB NOT NULL UNIQUE
);

CREATE TABLE indices (
	inode BIGINTEGER, 
	hash_id BIGINTEGER, 
	block_nr INTEGER, 
	PRIMARY KEY (inode, hash_id, block_nr)
);

CREATE TABLE options (
	name TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
