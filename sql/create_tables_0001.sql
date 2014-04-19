CREATE TABLE tree (
	id BIGINT PRIMARY KEY AUTO_INCREMENT, 
	parent_id BIGINT, 
	name BIGINT NOT NULL, 
	inode BIGINT NOT NULL, 
	UNIQUE (parent_id, name)
);

CREATE TABLE strings (
	id INTEGER PRIMARY KEY, 
	value TEXT(1024) NOT NULL UNIQUE
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
	`value` TEXT(100) NOT NULL
);
