SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

USE `DATABASE_NAME` ;

CREATE TABLE IF NOT EXISTS `DATABASE_NAME`.`copy_simple_table` (
  `primary_key_id` VARCHAR(100) NOT NULL,
  `int_field1` INT(11) NULL DEFAULT NULL,
  `decimal_field1` DECIMAL(10,0) NULL DEFAULT NULL,
  `datetime_field1` DATETIME NULL DEFAULT NULL,
  `bool_field1` TINYINT(1) NOT NULL DEFAULT 0,
  `varchar_field1` VARCHAR(1000) NOT NULL,
  `char_field1` CHAR(100) NULL DEFAULT NULL,
  PRIMARY KEY (`primary_key_id`))
ENGINE = InnoDB
AUTO_INCREMENT = 10007
DEFAULT CHARACTER SET utf8  COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `DATABASE_NAME`.`simple_table_2` (
  `primary_key_id` VARCHAR(100) NOT NULL,
  `int_field1` INT(11) NULL DEFAULT NULL,
  `decimal_field1` DECIMAL(10,0) NULL DEFAULT NULL,
  `datetime_field1` DATETIME NULL DEFAULT NULL,
  `bool_field1` TINYINT(1) NOT NULL DEFAULT 0,
  `varchar_field1` VARCHAR(1000) NOT NULL,
  `char_field1` CHAR(100) NULL DEFAULT NULL,
  PRIMARY KEY (`primary_key_id`))
ENGINE = InnoDB
AUTO_INCREMENT = 10007
DEFAULT CHARACTER SET utf8  COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `DATABASE_NAME`.`simple_table_3` (
  `primary_key_id` VARCHAR(100) NOT NULL,
  `int_field1` INT(11) NULL DEFAULT NULL,
  `decimal_field1` DECIMAL(10,0) NULL DEFAULT NULL,
  `datetime_field1` DATETIME NULL DEFAULT NULL,
  `bool_field1` TINYINT(1) NOT NULL DEFAULT 0,
  `varchar_field1` VARCHAR(1000) NOT NULL,
  `char_field1` CHAR(100) NULL DEFAULT NULL,
  PRIMARY KEY (`primary_key_id`))
ENGINE = InnoDB
AUTO_INCREMENT = 10007
DEFAULT CHARACTER SET utf8  COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `DATABASE_NAME`.`simple_table_4` (
  `primary_key_id` VARCHAR(100) NOT NULL,
  `int_field1` INT(11) NULL DEFAULT NULL,
  `decimal_field1` DECIMAL(10,0) NULL DEFAULT NULL,
  `datetime_field1` DATETIME NULL DEFAULT NULL,
  `bool_field1` TINYINT(1) NOT NULL DEFAULT 0,
  `varchar_field1` VARCHAR(1000) NOT NULL,
  `char_field1` CHAR(100) NULL DEFAULT NULL,
  PRIMARY KEY (`primary_key_id`))
ENGINE = InnoDB
AUTO_INCREMENT = 10007
DEFAULT CHARACTER SET utf8  COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `DATABASE_NAME`.`simple_table_5` (
  `primary_key_id` VARCHAR(100) NOT NULL,
  `int_field1` INT(11) NULL DEFAULT NULL,
  `decimal_field1` DECIMAL(10,0) NULL DEFAULT NULL,
  `datetime_field1` DATETIME NULL DEFAULT NULL,
  `bool_field1` TINYINT(1) NOT NULL DEFAULT 0,
  `varchar_field1` VARCHAR(1000) NOT NULL,
  `char_field1` CHAR(100) NULL DEFAULT NULL,
  PRIMARY KEY (`primary_key_id`))
ENGINE = InnoDB
AUTO_INCREMENT = 10007
DEFAULT CHARACTER SET utf8  COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `DATABASE_NAME`.`simple_table_6` (
  `primary_key_id` VARCHAR(100) NOT NULL,
  `int_field1` INT(11) NULL DEFAULT NULL,
  `decimal_field1` DECIMAL(10,0) NULL DEFAULT NULL,
  `datetime_field1` DATETIME NULL DEFAULT NULL,
  `bool_field1` TINYINT(1) NOT NULL DEFAULT 0,
  `varchar_field1` VARCHAR(1000) NOT NULL,
  `char_field1` CHAR(100) NULL DEFAULT NULL,
  PRIMARY KEY (`primary_key_id`))
ENGINE = InnoDB
AUTO_INCREMENT = 10007
DEFAULT CHARACTER SET utf8  COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `DATABASE_NAME`.`simple_table_7` (
  `primary_key_id` VARCHAR(100) NOT NULL,
  `int_field1` INT(11) NULL DEFAULT NULL,
  `decimal_field1` DECIMAL(10,0) NULL DEFAULT NULL,
  `datetime_field1` DATETIME NULL DEFAULT NULL,
  `bool_field1` TINYINT(1) NOT NULL DEFAULT 0,
  `varchar_field1` VARCHAR(1000) NOT NULL,
  `char_field1` CHAR(100) NULL DEFAULT NULL,
  PRIMARY KEY (`primary_key_id`))
ENGINE = InnoDB
AUTO_INCREMENT = 10007
DEFAULT CHARACTER SET utf8  COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `DATABASE_NAME`.`simple_table_8` (
  `primary_key_id` VARCHAR(100) NOT NULL,
  `int_field1` INT(11) NULL DEFAULT NULL,
  `decimal_field1` DECIMAL(10,0) NULL DEFAULT NULL,
  `datetime_field1` DATETIME NULL DEFAULT NULL,
  `bool_field1` TINYINT(1) NOT NULL DEFAULT 0,
  `varchar_field1` VARCHAR(1000) NOT NULL,
  `char_field1` CHAR(100) NULL DEFAULT NULL,
  PRIMARY KEY (`primary_key_id`))
ENGINE = InnoDB
AUTO_INCREMENT = 10007
DEFAULT CHARACTER SET utf8  COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `DATABASE_NAME`.`simple_table_9` (
  `primary_key_id` VARCHAR(100) NOT NULL,
  `int_field1` INT(11) NULL DEFAULT NULL,
  `decimal_field1` DECIMAL(10,0) NULL DEFAULT NULL,
  `datetime_field1` DATETIME NULL DEFAULT NULL,
  `bool_field1` TINYINT(1) NOT NULL DEFAULT 0,
  `varchar_field1` VARCHAR(1000) NOT NULL,
  `char_field1` CHAR(100) NULL DEFAULT NULL,
  PRIMARY KEY (`primary_key_id`))
ENGINE = InnoDB
AUTO_INCREMENT = 10007
DEFAULT CHARACTER SET utf8  COLLATE utf8_bin;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
