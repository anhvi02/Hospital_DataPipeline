CREATE TABLE Affiliated_With (   
                  Physician INTEGER NOT NULL     CONSTRAINT fk_Physician_EmployeeID REFERENCES Physician(EmployeeID),   
                  Department INTEGER NOT NULL     CONSTRAINT fk_Department_DepartmentID REFERENCES Department(DepartmentID),   
                  PrimaryAffiliation BOOLEAN NOT NULL,   
                  PRIMARY KEY(Physician, Department) )