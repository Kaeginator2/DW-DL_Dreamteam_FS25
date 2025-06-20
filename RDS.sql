CREATE TABLE IF NOT EXISTS `DreamTeamWarehouse`.`pollution_recordings` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `location` VARCHAR(255) NULL DEFAULT NULL,
  `latitude` DOUBLE NULL DEFAULT NULL,
  `longitude` DOUBLE NULL DEFAULT NULL,
  `year` VARCHAR(10) NULL DEFAULT NULL,
  `month` VARCHAR(10) NULL DEFAULT NULL,
  `day` VARCHAR(10) NULL DEFAULT NULL,
  `hour` VARCHAR(10) NULL DEFAULT NULL,
  `aqi` INT NULL DEFAULT NULL,
  `co` DOUBLE NULL DEFAULT NULL,
  `no` DOUBLE NULL DEFAULT NULL,
  `no2` DOUBLE NULL DEFAULT NULL,
  `o3` DOUBLE NULL DEFAULT NULL,
  `so2` DOUBLE NULL DEFAULT NULL,
  `pm2_5` DOUBLE NULL DEFAULT NULL,
  `pm10` DOUBLE NULL DEFAULT NULL,
  `nh3` DOUBLE NULL DEFAULT NULL,
  `dt` INT NULL DEFAULT NULL,
  PRIMARY KEY (`id`));

CREATE TABLE IF NOT EXISTS `DreamTeamWarehouse`.`weather_recordings` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `name` TEXT NULL DEFAULT NULL,
  `country` TEXT NULL DEFAULT NULL,
  `longitude` DOUBLE NULL DEFAULT NULL,
  `latitude` DOUBLE NULL DEFAULT NULL,
  `temperatur` DOUBLE NULL DEFAULT NULL,
  `pressure` INT NULL DEFAULT NULL,
  `humidity` INT NULL DEFAULT NULL,
  `visibility` INT NULL DEFAULT NULL,
  `wind_speed` DOUBLE NULL DEFAULT NULL,
  `clouds_all` INT NULL DEFAULT NULL,
  `dt` INT NULL DEFAULT NULL,
  `rain_1h` DOUBLE NULL DEFAULT NULL,
  `snow_1h` DOUBLE NULL DEFAULT NULL,
  `year` INT NULL DEFAULT NULL,
  `month` INT NULL DEFAULT NULL,
  `day` INT NULL DEFAULT NULL,
  `hour` INT NULL DEFAULT NULL,
  `airport_name` VARCHAR(255) NULL DEFAULT NULL,
  `temperature_celsius` FLOAT NULL DEFAULT NULL,
   PRIMARY KEY (`id`));

CREATE TABLE IF NOT EXISTS `DreamTeamWarehouse`.`flight_delays` (
   `id` INT NOT NULL AUTO_INCREMENT,
  `flight_day` DATE NULL DEFAULT NULL,
  `airport_code` VARCHAR(10) NULL DEFAULT NULL,
  `airport_name` VARCHAR(100) NULL DEFAULT NULL,
  `delay_index_arrivals` FLOAT NULL DEFAULT NULL,
  `delay_index_departures` FLOAT NULL DEFAULT NULL,
  `flight_number` VARCHAR(10) NULL DEFAULT NULL,
  `scheduled_departure` TIMESTAMP NULL DEFAULT NULL,
  `real_departure` TIMESTAMP NULL DEFAULT NULL,
  `delay_minutes` INT NULL DEFAULT NULL,
  PRIMARY KEY (`id`));

CREATE TABLE IF NOT EXISTS `DreamTeamWarehouse`.`airport` (
  `id` INT NULL DEFAULT NULL,
  `ident` VARCHAR(255) NULL DEFAULT NULL,
  `type` VARCHAR(255) NULL DEFAULT NULL,
  `name` VARCHAR(255) NULL DEFAULT NULL,
  `latitude_deg` DOUBLE NULL DEFAULT NULL,
  `longitude_deg` DOUBLE NULL DEFAULT NULL,
  `elevation_ft` VARCHAR(255) NULL DEFAULT NULL,
  `continent` VARCHAR(10) NULL DEFAULT NULL,
  `iso_country` VARCHAR(10) NULL DEFAULT NULL,
  `iso_region` VARCHAR(10) NULL DEFAULT NULL,
  `municipality` VARCHAR(255) NULL DEFAULT NULL,
  `scheduled_servi` VARCHAR(10) NULL DEFAULT NULL,
  `icao_code` VARCHAR(10) NULL DEFAULT NULL,
  `iata_code` VARCHAR(10) NOT NULL,
  `home_link` VARCHAR(2083) NULL DEFAULT NULL,
  PRIMARY KEY (`iata_code`));
