# Dapper Code Generator
Simple C# Code Generator to create Database Models and Dapper CRUD to reduce manual code entry and repetitive work.

This is currently being worked on. Use with knowledge that it may be unstable and/or may change. The technologies and patterns used are mostly for my own education and shouldn't necessarily be followed in enterprise software.

### Instructions
1. Clone or download the repository
2. Open the solution file (DapperCodeGenerator.sln) in Visual Studio 2017
3. Make sure the Web project is set as the startup project
4. Start/run with IIS
    * I run it with IIS Express from Visual Studio 2017, you can run it however you want
5. Navigate to the site
6. Select the Connection Type
    * If you want further support for an existing or any other DB types feel free to make a PR and/or submit a ticket, just know I won't prioritize it
7. Enter a valid Connection String for the selected Connection Type
8. Press the "Connect" button
    * If you get messages about there not being any databases, confirm your Connection Type and Connection String
9. Select any database you wish to generate code for
10. A table should be populated with a list of tables for the database you selected, click the "Data Model" or "Dapper" buttons to the right to generate code

![image](https://user-images.githubusercontent.com/9127996/34977996-fb4a9776-fa59-11e7-8978-229aea9b1ef7.png)

#### Data Models
This is to generate C# objects that directly reflect the database tables for use with Dapper (or anything really).

![image](https://user-images.githubusercontent.com/9127996/34978041-2c01d460-fa5a-11e7-9ccb-285736b38cf9.png)

#### Dapper
This is to generate the Dapper CRUD methods for interacting with the Database using Data Models generated and Dapper.

This isn't complete yet. Try the below alternatives for now or write your own...
* https://github.com/MoonStorm/Dapper.FastCRUD
* https://github.com/ericdc1/Dapper.SimpleCRUD

#### Other Projects:
Code Generator - https://github.com/kcrossman/CodeGenerator
