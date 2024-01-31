# Test Plan

## Add

- [x] ADD GROUP <group> TO USER <user>
- [x] ADD GROUP <group> WITH PERMISSION <permission> IN <db>
- [x] ADD GROUP <group> WITH PERMISSION <permission> IN <db>.<collection>
- [x] ADD ROLE <role> TO USER <user>
- [x] ADD ROLE <role> WITH PERMISSION <permission> IN <db>
- [x] ADD ROLE <role> WITH PERMISSION <permission> IN <db>.<collection>
- [x] ADD USER <user> WITH PERMISSION <permission> IN <db>
- [x] ADD USER <user> WITH PERMISSION <permission> IN <db>.<collection>

## Create

- [x] CREATE COLLECTION <collection> IN <db> WITH SCHEMA <json>
- [x] CREATE DATABASE <db>
- [x] CREATE USER <username> WITH PASSWORD <password>

## Delete
- [x] DELETE COLLECTION <collection> FROM <database>
- [x] DELETE DATABASE <db>
- [x] DELETE GROUP <group> FROM <db>
- [x] DELETE GROUP <group> FROM <db>.<collection>
- [x] DELETE GROUP <group> FROM USER <user>
- [x] DELETE RECORD FROM <db>.<collection> WHERE <filter>
- [x] DELETE ROLE <role> FROM <db>
- [x] DELETE ROLE <role> FROM <db>.<collection>
- [x] DELETE ROLE <role> FROM USER <user>
- [x] DELETE USER <user>
- [x] DELETE USER <user> FROM <db>
- [x] DELETE USER <user> FROM <db>.<collection>

## Get
- [x] GET COLLECTION FROM <database>
- [x] GET DATABASE
- [x] GET RECORD FROM <db>.<collection>
- [x] GET RECORD FROM <db>.<collection> WHERE <filter>
- [x] GET RECORD FROM <db>.<collection> ORDER ASCENDING <field>
- [ ] GET RECORD FROM <db>.<collection> WHERE <filter> ORDER ASCENDING <field>
- [x] GET RECORD FROM <db>.<collection> ORDER DESCENDING <field>
- [ ] GET RECORD FROM <db>.<collection> WHERE <filter> ORDER DESCENDING <field>
- [x] GET SCHEMA <collection> FROM <database>
- [x] GET USER

## Insert
- [x] INSERT RECORD <json> INTO <db>.<collection>

## Update
- [x] UPDATE GROUP <group> WITH PERMISSION <permission> IN <db>.<collection>
- [x] UPDATE RECORD IN <db>.<collection> WITH <json to update> WHERE <filter>
- [x] UPDATE ROLE <role> WITH PERMISSION <permission> IN <db>.<collection>
- [x] UPDATE USER <user> WITH PERMISSION <permission> IN <db>.<collection>
- [x] UPDATE USER <username> WITH PASSWORD <password>
