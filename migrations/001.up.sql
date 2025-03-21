BEGIN;
CREATE TABLE schools (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE terms (
    year INT,
    season TEXT,

    PRIMARY KEY (year, season)
);

CREATE TABLE term_collections (
    id TEXT,
    school_id TEXT,

    year INT NOT NULL,
    season TEXT NOT NULL,
    name TEXT,
    still_collecting BOOL NOT NULL,
    FOREIGN KEY (school_id) REFERENCES schools(id),
    FOREIGN KEY (year, season) REFERENCES terms(year, season),
    PRIMARY KEY (id, school_id)
);

CREATE TABLE previous_collections (
    time_of_collection TIMESTAMP WITH TIME ZONE PRIMARY KEY,
    was_success BOOL NOT NULL
);

CREATE TABLE professors (
    id TEXT,
    school_id TEXT,

    name TEXT NOT NULL,
    email_address TEXT,
    first_name TEXT,
    last_name TEXT,
    FOREIGN KEY (school_id) REFERENCES schools(id),
    PRIMARY KEY (id, school_id)
);

CREATE TABLE courses (
    school_id TEXT,
    subject_code TEXT,
    number TEXT,

    subject_description TEXT,
    title TEXT,
    description TEXT,
    credit_hours REAL NOT NULL,
    FOREIGN KEY (school_id) REFERENCES schools(id),
    PRIMARY KEY (school_id, subject_code, number)
);

CREATE TABLE sections (
    sequence TEXT,
    term_collection_id TEXT,
    subject_code TEXT,
    course_number TEXT,
    school_id TEXT,

    max_enrollment INTEGER,
    instruction_method TEXT,
    campus TEXT,
    enrollment INTEGER,
    primary_professor_id TEXT,
    FOREIGN KEY (school_id, subject_code, course_number) 
        REFERENCES courses(school_id, subject_code, number),
    FOREIGN KEY (primary_professor_id, school_id) REFERENCES professors(id, school_id),

    FOREIGN KEY (term_collection_id, school_id) REFERENCES term_collections(id, school_id),
    PRIMARY KEY (sequence, term_collection_id, subject_code, course_number, school_id)
);


CREATE TABLE meeting_times (
    sequence INT,
    section_sequence TEXT,
    term_collection_id TEXT,
    subject_code TEXT,
    course_number TEXT,
    school_id TEXT,

    start_date TIMESTAMP,
    end_date TIMESTAMP,
    meeting_type TEXT,
    start_minutes TIME,
    end_minutes TIME,
    is_monday BOOLEAN NOT NULL,
    is_tuesday BOOLEAN NOT NULL,
    is_wednesday BOOLEAN NOT NULL,
    is_thursday BOOLEAN NOT NULL,
    is_friday BOOLEAN NOT NULL,
    is_saturday BOOLEAN NOT NULL,
    is_sunday BOOLEAN NOT NULL,
    FOREIGN KEY (section_sequence, term_collection_id, school_id, subject_code, course_number)
        REFERENCES sections(sequence, term_collection_id, school_id, subject_code, course_number) ON DELETE CASCADE,
    PRIMARY KEY (sequence, section_sequence, term_collection_id, subject_code, course_number, school_id)
);

COMMIT;
