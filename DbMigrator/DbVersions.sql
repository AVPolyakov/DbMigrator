CREATE TABLE DbVersions (
  Id int IDENTITY,
  DbVersion int NOT NULL,
  ScriptName nvarchar(255) NOT NULL,
  CreationDate datetime NOT NULL CONSTRAINT DF_DbVersions_CreationDate DEFAULT (GETDATE()),
  CONSTRAINT PK_DbVersions_Id PRIMARY KEY CLUSTERED (Id)
) ON [PRIMARY]
GO

INSERT INTO [DbVersions]
           ([DbVersion]
           ,[ScriptName])
     VALUES
           (1,
           'Init.sql')
GO