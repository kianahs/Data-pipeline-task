-- create database
DROP DATABASE IF EXISTS datasets;
CREATE DATABASE datasets;

USE datasets;

-- create tables

CREATE TABLE `article` (
  `article_uuid` varchar(256) NOT NULL,
  `article_title` text,
  `article_abstract` text,
  UNIQUE KEY `article_uuid` (`article_uuid`)
);

-- add foreign key constraint for uuid 
CREATE TABLE `relationship` (
  `article_uuid` varchar(256) NOT NULL,
  `cause_concept_name` text,
  `effect_concept_name` text,
  KEY `foreign_key_article_uuid_relationship` (`article_uuid`),
  CONSTRAINT `foreign_key_article_uuid_relationship` FOREIGN KEY (`article_uuid`) REFERENCES `article` (`article_uuid`) ON DELETE CASCADE ON UPDATE CASCADE
);
-- add foreign key constraint for uuid 
CREATE TABLE `tag` (
  `article_uuid` varchar(256) NOT NULL,
  `article_tag` text,
  KEY `foreign_key_article_uuid_tag` (`article_uuid`),
  CONSTRAINT `foreign_key_article_uuid_tag` FOREIGN KEY (`article_uuid`) REFERENCES `article` (`article_uuid`) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE `data_final` (
  `article_data` json DEFAULT NULL
);


DELIMITER $$

-- create procedures

-- import procedures : 
-- create a procedure to insert values of each dataframe to its corresponding table

CREATE PROCEDURE `InsertArticles`(IN i_article_uuid VARCHAR(256), IN i_article_title TEXT, IN i_article_abstract TEXT)
BEGIN
    INSERT INTO article (article_uuid, article_title, article_abstract) 
    VALUES (i_article_uuid, i_article_title, i_article_abstract);
END $$

CREATE PROCEDURE `InsertRelationships`(IN i_article_uuid VARCHAR(256), IN i_cause_concept_name TEXT, IN i_effect_concept_name TEXT)
BEGIN
    INSERT INTO relationship (article_uuid, cause_concept_name, effect_concept_name) 
    VALUES (i_article_uuid, i_cause_concept_name, i_effect_concept_name);
END $$

CREATE PROCEDURE `InsertTags`(IN i_article_uuid VARCHAR(256), IN i_article_tag TEXT)
BEGIN
    INSERT INTO tag (article_uuid, article_tag) 
    VALUES (i_article_uuid, i_article_tag);
END $$

-- transform procedure:
-- To aggregate data,
-- transform them to the defined schema,
-- and insert it to the data_final table

CREATE PROCEDURE `CombineData`()
BEGIN
    INSERT INTO data_final (article_data)
    -- create a json object
    SELECT JSON_OBJECT(
        'article_uuid', A.article_uuid,
        'article_title', A.article_title,
        'article_abstract', A.article_abstract,
        'relationships', (
            -- create a JSON array of the JSON objects (defined below)
            -- each object contains information from relationship table
            -- we only pick elements of the relationship table for json object with the same article_uuid
            SELECT JSON_ARRAYAGG(
                JSON_OBJECT(
                    'cause_concept_name', R.cause_concept_name,
                    'effect_concept_name', R.effect_concept_name
                )
            )
            FROM relationship R
            WHERE R.article_uuid = A.article_uuid
        ),
        -- create array of tags with the same article_uuid 
        'tags', (
            SELECT JSON_ARRAYAGG(T.article_tag)
            FROM tag T 
            WHERE T.article_uuid = A.article_uuid
        )
    )
    FROM article A;
END $$

-- export procedure:
-- to get the data from the data_final table
CREATE PROCEDURE `GetDataFinal`()
BEGIN
    SELECT article_data
    FROM data_final;
END $$


-- Combined transfrom and export (solution 2) with transaction handling
-- all operations within the transaction are completed successfully; otherwise, if any errors occur, both operations are canceled.
CREATE PROCEDURE `TransformExport`()
BEGIN
    -- - If an exception happens:
    -- rollback any changes

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
    END;
    
    START TRANSACTION;

    -- combine the data and insert it to the data_final table
    CALL CombineData();
    --  export results
    CALL GetDataFinal();
    -- commit changes
    COMMIT;
END $$

DELIMITER ;
