-- Create the root node of the file system?
INSERT INTO strings (id, value) VALUES (1, '');
INSERT INTO tree (id, parent_id, name, inode) VALUES (1, NULL, 1, 1);
