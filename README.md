# Ceres

## About
Ceres is a database system designed to allow for the storage and retrieval of semi-structured data. I.e. one that conforms to a "top-level schema" where columns types are known, but those columns can in-turn contain dictionaries or lists.

### Naming
Ceres is named after the Roman goddess of agriculture for the way in which the system "harvests" data from the files it is stored on

## How It Works
** WIP **

## To Do
- [x] Basic reading from structure
- [x] Basic writing to structure
- [x] Basic deletion from structure
- [x] AQL query parsing
    - [x] Basic query parsing
    - [x] Nested conditionals with parenthesis
- [ ] Index management
    - [x] Basic indices
    - [ ] Multi-word string indices
    - [ ] List indices
    - [ ] Dict indices
- [x] Collection schema
    - [x] Schema definition on collection creation
    - [x] Schema modification
- [x] Query parsing/response
    - [x] GET
        - [x] Column filtering
    - [x] POST
    - [x] PATCH
    - [x] PUT
    - [x] DELETE
    - [x] FILTER
    - [x] LIMIT
    - [x] ORDERASC
    - [x] ORDERDSC
    - [x] DBADD
    - [x] DBDEL
    - [ ] DBGET
    - [x] COLADD
    - [x] COLDEL
    - [x] COLMOD
    - [ ] COLGET
    - [x] PERMITADD
    - [x] PERMITDEL
    - [x] PERMITMOD
    - [x] PERMITGET
    - [x] USERADD
    - [x] USERDEL
    - [x] USERMOD
    - [x] USERGET
    - [x] COUNT
- [x] UDP server
- [x] Authentication
    - [x] User management
    - [x] User roles
- [x] Break free space out into separate package
- [x] Validation
    - [x] Validate schema values
    - [x] Validate data values against schema
- [x] Direct access protection
    - [x] _auth database
    - [x] _user collections
- [x] Concurrent connection support via read/write queue

## Contact
This software is written by John Carter. If you have any questions or concerns feel free to create an issue on GitHub or send me an email at jfcarter2358(at)gmail.com
