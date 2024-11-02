

--The below is MySQL Scripts using for RDBS job history


WITH employee_sales AS (
    SELECT
        e.employee_id,
        e.first_name,
        e.last_name,
        e.department_id,
        d.department_name,
        s.sale_id,
        s.sale_date,
        s.customer_id,
        c.customer_name,
        s.product_id,
        p.product_name,
        p.category_id,
        cat.category_name,
        s.quantity,
        s.sale_amount,
        ROW_NUMBER() OVER(PARTITION BY s.customer_id ORDER BY s.sale_date DESC) AS recent_sale_rank
    FROM
        employees e
    JOIN
        departments d ON e.department_id = d.department_id
    JOIN
        sales s ON e.employee_id = s.employee_id
    JOIN
        customers c ON s.customer_id = c.customer_id
    JOIN
        products p ON s.product_id = p.product_id
    JOIN
        categories cat ON p.category_id = cat.category_id
    WHERE
        s.sale_date BETWEEN '2023-01-01' AND '2023-12-31'
),
customer_purchases AS (
    SELECT
        customer_id,
        SUM(quantity) AS total_quantity,
        SUM(sale_amount) AS total_sales,
        COUNT(DISTINCT product_id) AS unique_products
    FROM
        employee_sales
    GROUP BY
        customer_id
));





drop table if exists etl_audit_db.etl_step_audit;
drop table  etl_audit_db.etl_batch_audit  ;
drop TABLE  etl_audit_db.etl_job_audit;
drop table if exists etl_audit_db.etl_step_audit;


drop table if exists etl_audit_db.etl_notification_log;



drop table etl_audit_db.etl_error_log;
drop table etl_audit_db.etl_step_audit;

drop table etl_audit_db.etl_batch_audit  ;
drop TABLE etl_audit_db.etl_job_audit;

drop table etl_audit_db.etl_job_status  ;
drop table etl_audit_db.etl_job_stats ;
drop table etl_audit_db.etl_notification_log;
drop table etl_audit_db.etl_status;




create database etl_audit_db;

use etl_audit_db;


CREATE TABLE etl_audit_db.etl_job_stats (
    job_id VARCHAR(255) PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    submitted_at TIMESTAMP NOT NULL,
    pre_run_started_at TIMESTAMP,
    running_started_at TIMESTAMP,
    completed_at TIMESTAMP,
    failed_at TIMESTAMP,
    status VARCHAR(50) NOT NULL
);



CREATE TABLE  etl_audit_db.etl_job_status (
    status VARCHAR(50) PRIMARY KEY
);



CREATE TABLE  etl_audit_db.etl_job_audit (
    job_id VARCHAR(255) PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    submitted_by VARCHAR(255) NOT NULL,
    submitted_at TIMESTAMP NOT NULL,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    error_message TEXT
);


CREATE TABLE  etl_audit_db.etl_batch_audit (
    batch_id VARCHAR(255) PRIMARY KEY,
    job_id VARCHAR(255) NOT NULL,
    batch_name VARCHAR(255) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    error_message TEXT
);


CREATE TABLE etl_audit_db.etl_step_audit (
    step_id  VARCHAR(255) PRIMARY KEY,
    batch_id VARCHAR(255) NOT NULL,
    step_name VARCHAR(255) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    row_count INT,
    error_message TEXT
);


CREATE TABLE  etl_audit_db.etl_error_log (
    error_id SERIAL PRIMARY KEY,
    job_id VARCHAR(255),
    batch_id VARCHAR(255),
    step_id BIGINT UNSIGNED,
    error_timestamp TIMESTAMP NOT NULL,
    error_message TEXT NOT NULL,
    error_details TEXT
);

CREATE TABLE  etl_audit_db.etl_status (
    status VARCHAR(50) PRIMARY KEY
);

CREATE TABLE  etl_audit_db.etl_notification_log (
    notification_id VARCHAR(255) PRIMARY KEY,
    job_id VARCHAR(255),
    batch_id VARCHAR(255),
    step_id INT,
    notification_type VARCHAR(50) NOT NULL,
    recipient VARCHAR(255) NOT NULL,
    sent_at TIMESTAMP NOT NULL,
    message TEXT NOT NULL
);


-- Populate etl_status table with possible statuses
INSERT INTO etl_status (status) VALUES ('Submitted');
INSERT INTO etl_status (status) VALUES ('Running');
INSERT INTO etl_status (status) VALUES ('Success');
INSERT INTO etl_status (status) VALUES ('Failed');

-- Example of inserting an ETL job audit entry
INSERT INTO etl_job_audit (
    job_id, job_name, submitted_by, submitted_at, status
) VALUES (
    'job_123', 'Daily Sales ETL', 'user_name', CURRENT_TIMESTAMP, 'Submitted'
);

-- Example of inserting a batch audit entry
INSERT INTO etl_batch_audit (
    batch_id, job_id, batch_name, started_at, status
) VALUES (
    'batch_001', 'job_123', 'Sales Data Batch 1', CURRENT_TIMESTAMP, 'Running'
);

-- Example of inserting a step audit entry
INSERT INTO etl_step_audit (
    batch_id, step_name, started_at, status
) VALUES (
    'batch_001', 'Extract Sales Data', CURRENT_TIMESTAMP, 'Running'
);

-- Example of inserting an error log entry
INSERT INTO etl_error_log (
    job_id, batch_id, step_id, error_timestamp, error_message, error_details
) VALUES (
    'job_123', 'batch_001', 1, CURRENT_TIMESTAMP, 'Null pointer exception', 'Detailed stack trace or error context'
);

-- Example of inserting a notification log entry
INSERT INTO etl_notification_log (
    job_id, batch_id, step_id, notification_type, recipient, sent_at, message
) VALUES (
    'job_123', 'batch_001', 1, 'Email', 'admin@example.com', CURRENT_TIMESTAMP, 'ETL job "Daily Sales ETL" started.'
);






