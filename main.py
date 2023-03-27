import sys
sys.path.append('..')

import lib.rel_algebra_calculus.rel_algebra_calculus as ra

def ha2(univDB):
    tables = univDB["tables"]
    department = tables["department"]
    course = tables["course"]
    prereq = tables["prereq"]
    class_ = tables["class"]
    faculty = tables["faculty"]
    student = tables["student"]
    enrollment = tables["enrollment"]
    transcript = tables["transcript"]

    # query_a ---------------------------------------------------------------
    r1 = ra.join(transcript, student)
    dcodeIsCSAndcnoIs530 = ra.sel(r1,  lambda x: (x["dcode"] == "CS" and x["cno"] == 530))
    query_a = ra.proj(dcodeIsCSAndcnoIs530, ['ssn', 'name', 'major', 'status'])
    # -----------------------------------------------------------------------

    # query_b ---------------------------------------------------------------
    r2 = ra.join(transcript, student)
    dcodeIsCSAndcnoIs530AndNamedJohn = ra.sel(r2, lambda x: (x["dcode"] == "CS" and x["cno"] == 530 and x["name"] == "John"))
    query_b = ra.proj(dcodeIsCSAndcnoIs530AndNamedJohn, ['ssn', 'name', 'major', 'status'])
    # -----------------------------------------------------------------------

    # query_c ---------------------------------------------------------------
    r1 = ra.join(enrollment, class_)
    r2 = ra.join(r1, prereq)
    r3 = ra.proj(r2, ["ssn", "pcode", "pno"])
    SP = ra.ren(r3, {"pcode": "dcode", "pno": "cno"} )
    cond_a_or_b = lambda t: (t["grade"] == "A" or t["grade"] == "B")
    r4 = ra.sel(transcript, cond_a_or_b)
    ST = ra.proj(r4, ["ssn", "dcode", "cno"])
    NOT = ra.proj(ra.diff(SP, ST), ["ssn"] )
    YES = ra.diff( ra.proj(student, ["ssn"]),  NOT)
    query_c = ra.join(student, YES)
    # -----------------------------------------------------------------------

    # query_d ---------------------------------------------------------------
    r1 = ra.join(enrollment, class_)
    r2 = ra.join(r1, prereq)
    r3 = ra.proj(r2, ["ssn", "pcode", "pno"])
    SP = ra.ren(r3, {"pcode": "dcode", "pno": "cno"} )
    cond_a_or_b = lambda t: (t["grade"] == "A" or t["grade"] == "B")
    r4 = ra.sel(transcript, cond_a_or_b)
    ST = ra.proj(r4, ["ssn", "dcode", "cno"])
    NOT = ra.proj(ra.diff(SP, ST), ["ssn"] )
    query_d = ra.join(student, NOT)
    # -----------------------------------------------------------------------

    # query_e ---------------------------------------------------------------
    r1 = ra.join(enrollment, class_)
    r2 = ra.join(r1, prereq)
    r3 = ra.proj(r2, ["ssn", "pcode", "pno"])
    SP = ra.ren(r3, {"pcode": "dcode", "pno": "cno"} )
    cond_a_or_b = lambda t: (t["grade"] == "A" or t["grade"] == "B")
    r4 = ra.sel(transcript, cond_a_or_b)
    ST = ra.proj(r4, ["ssn", "dcode", "cno"])
    NOT = ra.proj(ra.diff(SP, ST), ["ssn"])
    student_john = ra.sel(student, lambda t: (t["name"] == "John"))
    query_e = ra.join(student_john, NOT)
    # -----------------------------------------------------------------------

    # query_f ---------------------------------------------------------------
    j1 = ra.join(course, prereq)
    t1 = ra.proj(j1, ["dcode", "cno"])
    all_courses = ra.proj(course, ['dcode', 'cno'])
    query_f = ra.diff(all_courses, t1)
    # -----------------------------------------------------------------------

    # query_g ---------------------------------------------------------------
    query_g = ra.proj(ra.join(course, prereq), ["dcode", "cno"])
    # -----------------------------------------------------------------------

    # query_h ---------------------------------------------------------------
    query_h = ra.proj(ra.join(class_, prereq), ["class", "dcode", "cno", "instr"])
    # -----------------------------------------------------------------------

    # query_i ---------------------------------------------------------------
    transcript_proj = ra.proj(transcript, ['dcode', 'cno', 'ssn'])
    cond_a_or_b = lambda t: (t["grade"] == "A" or t["grade"] == "B")
    transcript_AorB = ra.proj(ra.sel(transcript, cond_a_or_b), ["dcode", "cno", "ssn"])
    NOT = ra.proj(ra.diff(transcript_proj, transcript_AorB),  ["ssn"] )
    YES = ra.diff(ra.proj(student, ["ssn"]),  NOT)
    query_i = ra.join(student, YES)
    # -----------------------------------------------------------------------

    # query_j ---------------------------------------------------------------
    brodsky = ra.proj(ra.sel(faculty, lambda x: x["name"] == "Brodsky"), ["ssn"])
    brodsky_classes = ra.proj(ra.sel(ra.join(brodsky, class_), lambda x: x["instr"] == x["ssn"]), ["class"])
    brodsky_classes_class = ra.proj(brodsky_classes, ["class"])
    result = ra.join(enrollment, brodsky_classes_class)
    result1 = ra.join(result, student)
    query_j = ra.proj(result1, ['ssn', 'name', 'major', 'status'])
    # -----------------------------------------------------------------------

    # query_k ---------------------------------------------------------------
    classes = ra.proj(class_, ["class"])
    tempssn = ra.div(enrollment, classes, ["class"])
    result = ra.join(tempssn, student)
    query_k = ra.proj(result, ['ssn'])
    # -----------------------------------------------------------------------

    # query_l ---------------------------------------------------------------
    cs_students = ra.sel(student, lambda x: x["major"] == "CS")
    classes = ra.proj(class_, ["class", "dcode"])
    math_only = ra.sel(classes,  lambda x: x["dcode"] == "MTH")
    math_only_class = ra.proj(math_only, ["class"])
    tempssn = ra.div(enrollment, math_only_class, ["class"])
    result = ra.join(tempssn, cs_students)
    query_l = ra.proj(result, ["ssn"])
    # -----------------------------------------------------------------------

    # ---------------------------------------------------------------
    # Some post-processing which you do not need to worry about
    # Do not change anything after this

    query_a = ra.distinct(query_a)
    query_b = ra.distinct(query_b)
    query_c = ra.distinct(query_c)
    query_d = ra.distinct(query_d)
    query_e = ra.distinct(query_e)
    query_f = ra.distinct(query_f)
    query_g = ra.distinct(query_g)
    query_h = ra.distinct(query_h)
    query_i = ra.distinct(query_i)
    query_j = ra.distinct(query_j)
    query_k = ra.distinct(query_k)
    query_l = ra.distinct(query_l)


    ra.sortTable(query_a,["ssn"])
    ra.sortTable(query_b,["ssn"])
    ra.sortTable(query_c, ['ssn'])
    ra.sortTable(query_d, ['ssn'])
    ra.sortTable(query_e, ['ssn'])
    ra.sortTable(query_f, ['dcode', 'cno'])
    ra.sortTable(query_g, ['dcode', 'cno'])
    ra.sortTable(query_h, ['class'])
    ra.sortTable(query_i, ['ssn'])
    ra.sortTable(query_j, ['ssn'])
    ra.sortTable(query_k, ['ssn'])
    ra.sortTable(query_l, ['ssn'])

    return({
        "query_a": query_a,
        "query_b": query_b,
        "query_c": query_c,
        "query_d": query_d,
        "query_e": query_e,
        "query_f": query_f,
        "query_g": query_g,
        "query_h": query_h,
        "query_i": query_i,
        "query_j": query_j,
        "query_k": query_k,
        "query_l": query_l
    })
