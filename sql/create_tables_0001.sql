CREATE TABLE tree (
	id BIGINT PRIMARY KEY AUTO_INCREMENT, 
	parent_id BIGINT, 
	name BIGINT NOT NULL, 
	inode BIGINT NOT NULL, 
	UNIQUE (parent_id, name)
);

CREATE TABLE strings (
	id INTEGER PRIMARY KEY, 
	value TEXT(1024) NOT NULL
);
	
CREATE TABLE inodes (
	inode BIGINT PRIMARY KEY,
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
	inode BIGINT UNIQUE
	target BLOB NOT NULL
);

CREATE TABLE hashes (
	id BIGINT PRIMARY KEY,
	hash BLOB NOT NULL UNIQUE
);

CREATE TABLE indices (
	inode BIGINT,
	hash_id BIGINT,
	block_nr INTEGER, 
	PRIMARY KEY (inode, hash_id, block_nr)
);

CREATE TABLE options (
	name VARCHAR PRIMARY KEY,
	value VARCHAR NOT NULL
);
