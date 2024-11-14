SELECT COUNT(*) AS TotalDuplicates 
    FROM (SELECT *, COUNT(*) OVER (PARTITION BY AppointmentID, Patient, PrepNurse, Physician, [Start], [End], ExaminationRoom) AS cnt FROM Appointment) AS DupCheck
    WHERE cnt > 1