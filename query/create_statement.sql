CREATE TABLE `table_trips` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `origin_coord1` varchar(45) NOT NULL,
  `origin_coord2` varchar(45) NOT NULL,
  `destination_coord1` varchar(45) NOT NULL,
  `destination_coord2` varchar(45) NOT NULL,
  `datetime` varchar(45) NOT NULL,
  `region` varchar(45) NOT NULL,
  `datasource` varchar(45) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id_UNIQUE` (`id`),
  UNIQUE KEY `unique_index` (`origin_coord1`,`origin_coord2`,`destination_coord1`,`destination_coord2`,`datetime`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `table_staging` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `origin_coord1` varchar(45) NOT NULL,
  `origin_coord2` varchar(45) NOT NULL,
  `destination_coord1` varchar(45) NOT NULL,
  `destination_coord2` varchar(45) NOT NULL,
  `datetime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `region` varchar(45) NOT NULL,
  `datasource` varchar(45) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id_UNIQUE` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=201 DEFAULT CHARSET=latin1;
