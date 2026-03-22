CREATE TABLE IF NOT EXISTS employees_clean (
    employee_id     INTEGER PRIMARY KEY,
    first_name      VARCHAR(50) NOT NULL,
    last_name       VARCHAR(50) NOT NULL,
    full_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(100) UNIQUE NOT NULL,
    email_domain    VARCHAR(50),
    hire_date       DATE NOT NULL,
    job_title       VARCHAR(100),
    department      VARCHAR(50),
    salary          DECIMAL(10,2),
    salary_band     VARCHAR(20),
    manager_id      INTEGER,
    address         TEXT,
    city            VARCHAR(50),
    state           VARCHAR(2),
    zip_code        VARCHAR(10),
    birth_date      DATE,
    age             INTEGER,
    tenure_years    DECIMAL(3,1),
    status          VARCHAR(20) DEFAULT 'Active',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS employees_rejected (
    raw_employee_id VARCHAR(50),
    rejection_reason TEXT,
    raw_data        TEXT,
    rejected_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_department  ON employees_clean(department);
CREATE INDEX IF NOT EXISTS idx_status      ON employees_clean(status);
CREATE INDEX IF NOT EXISTS idx_salary_band ON employees_clean(salary_band);
CREATE INDEX IF NOT EXISTS idx_hire_date   ON employees_clean(hire_date);


CREATE OR REPLACE VIEW dept_summary AS
SELECT
    department,
    COUNT(*)                    AS headcount,
    ROUND(AVG(salary), 2)       AS avg_salary,
    MIN(salary)                 AS min_salary,
    MAX(salary)                 AS max_salary,
    ROUND(AVG(tenure_years), 1) AS avg_tenure_years
FROM employees_clean
GROUP BY department
ORDER BY headcount DESC;

CREATE OR REPLACE VIEW salary_band_summary AS
SELECT
    salary_band,
    COUNT(*)              AS headcount,
    ROUND(AVG(salary), 2) AS avg_salary
FROM employees_clean
GROUP BY salary_band
ORDER BY avg_salary;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin;