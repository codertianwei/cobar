CREATE TABLE `cobar_config` (
  `id` int(10) NOT NULL DEFAULT '1',
  `schema` varchar(128) NOT NULL DEFAULT '0',
  `data_node` varchar(128) NOT NULL DEFAULT '',
  `tables` varchar(128) NOT NULL DEFAULT '',
  `data_source` varchar(128) NOT NULL DEFAULT '',
  `db_host` varchar(128) NOT NULL DEFAULT '',
  `db_port` int(10) NOT NULL DEFAULT '3306',
  `db_database` varchar(128) NOT NULL DEFAULT '',
  `db_user` varchar(128) NOT NULL DEFAULT '',
  `db_password` varchar(128) NOT NULL DEFAULT '',
  `sql_mode` varchar(128) NOT NULL DEFAULT '',
  `create_time` int(10) NOT NULL DEFAULT '0',
  `update_time` int(10) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;
