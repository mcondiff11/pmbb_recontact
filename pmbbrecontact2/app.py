from flask import Flask, render_template_string, render_template, request, url_for,redirect
from databricks import sql
import pandas as pd
import os
import math
from databricks.sdk import WorkspaceClient
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# Configuration
ROWS_PER_PAGE = 100

# --- Helper Function to Get User Email ---
def get_current_databricks_user_email():
    """
    Retrieves the end user's email from the X-Forwarded-Email header, 
    relying on the Flask request context.
    """
    try:
        # 'request' is a thread-local proxy provided by Flask, 
        # which automatically resolves to the request currently being processed.
        user_email = request.headers.get("X-Forwarded-Email")
        return user_email
    except RuntimeError:
        # This handles cases where the function is called outside of an active request context
        print("ERROR: get_current_databricks_user_email called outside of Flask request context.")
        return None

def get_databricks_connection():
    """Create and return a Databricks SQL connection"""
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_ACCESS_TOKEN"]
    )

# Set up Databricks connection
@app.route('/', methods=["GET", "POST"])
def index():
    try:

       # Get page number from URL parameter, default to 1
        current_page = request.args.get('page', 1, type=int)

        status_filter = request.args.get('status_filter')
        confirmed_appointments = status_filter == '1'

        if confirmed_appointments:
            where = "WHERE AppointmentConfirmationStatus = 'Confirmed'"
            pagination_vars = "&status_filter=1"
        else:
            where = ""
            pagination_vars = ""
        
        
        # Calculate offset for SQL query
        offset = (current_page - 1) * ROWS_PER_PAGE
        
        with get_databricks_connection() as connection:
            with connection.cursor() as cursor:
                
                count_query = f"SELECT COUNT(*) FROM biobank_analytics.pmbb_saliva.upcoming_appointments_for_saliva {where}"
                cursor.execute(count_query)
                total_rows = cursor.fetchone()[0]
                
                # Calculate pagination info
                total_pages = math.ceil(total_rows / ROWS_PER_PAGE)

                has_prev = current_page > 1
                has_next = current_page < total_pages
                prev_num = current_page - 1 if has_prev else None
                next_num = current_page + 1 if has_next else None


                # Your SQL query - replace with your actual table
                # Build main query with LIMIT and OFFSET
               

                query = f"""
                SELECT EMPI, Patient_name,AppointmentInstant,AppointmentConfirmationStatus,DepartmentEpicId,Appointment_Location FROM biobank_analytics.pmbb_saliva.upcoming_appointments_for_saliva {where} ORDER BY AppointmentInstant LIMIT {ROWS_PER_PAGE} OFFSET {offset}
                """
                cursor.execute(query)
                
                # Fetch column names
                columns = [desc[0] for desc in cursor.description]
                # Fetch all results
                rows = cursor.fetchall()

                # Convert to DataFrame for HTML rendering
                df = pd.DataFrame(rows, columns=columns)
                table_html = df.to_html(classes='table table-striped table-hover', 
                                      table_id='data-table',
                                      escape=False,
                                      index=False)
                
              
                
                # Calculate pagination range for display
                start_page = max(1, current_page - 2)
                end_page = min(total_pages + 1, current_page + 3)
                page_range = range(start_page, end_page)
                


                return render_template('index.html',  rows=rows,
                columns=columns,
                current_page=current_page,
                total_pages=total_pages,
                total_rows=total_rows,
                start_row=(current_page - 1) * ROWS_PER_PAGE + 1,
                end_row=min(current_page * ROWS_PER_PAGE, total_rows),
                page_range=page_range,
                start_page=start_page,
                end_page=end_page,
                pagination_vars=pagination_vars,
                confirmed_appointments=confirmed_appointments)
                
    except Exception as e:
        return f"<h1>Error connecting to database:</h1><p>{str(e)}</p>"
    
@app.route('/person/<person_id>')
def person_details(person_id):
    """Page showing all rows for a specific person_id"""
    try:
        connection = get_databricks_connection()
        cursor = connection.cursor()

        status_filter = request.args.get('status_filter')
        confirmed_appointments = status_filter == '1'

        if confirmed_appointments:
            where = f" EMPI = {person_id} AND AppointmentConfirmationStatus = 'Confirmed'"
            pagination_vars = "&status_filter=1"
        else:
            where = f" EMPI = {person_id}"
            pagination_vars = ""
        
        # Query to get all rows for specific person_id
        query = f"SELECT EMPI, Patient_name,Patient_home_phone,Patient_cell_phone,Patient_email,AppointmentDate,AppointemntTime,AppointmentConfirmationStatus,Appointment_Location,DepartmentEpicId,EncounterEpicCsn FROM biobank_analytics.pmbb_saliva.upcoming_appointments_for_saliva where {where} ORDER BY AppointmentInstant"
        cursor.execute(query, (person_id,))
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        where1 = where = f" EMPI = {person_id}"
        substudy_query = f"select * from biobank_analytics.pmbb_saliva.substudy_cohorts where {where1}"
        cursor.execute(substudy_query, (person_id))
        substudy_rows = cursor.fetchall()

        contact_query = f"select * from biobank_analytics.pmbb_saliva.recontact where empi_id = {person_id}"
        cursor.execute(contact_query, (person_id))
        contact_rows = cursor.fetchall()

        scheduled_collection_query = f"select * from biobank_analytics.pmbb_saliva.scheduled_collection where EMPI = {person_id}"
        cursor.execute(scheduled_collection_query, (person_id))
        scheduled_collection_rows = cursor.fetchall()

        cursor.close()
        connection.close()

        return render_template('person.html', rows=rows,columns=columns,
                confirmed_appointments=confirmed_appointments,
                person_id=person_id, substudies=substudy_rows, contacts=contact_rows, collections=scheduled_collection_rows) 
    except Exception as e:
            return f"Error: {str(e)}"

@app.route('/locations', methods=['GET', 'POST'])
def get_locations():
    """Jawn for the locations"""
    try:
    
        connection = get_databricks_connection()
        cursor = connection.cursor()

        query = "SELECT DepartmentEpicId, DepartmentName,DepartmentExternalName,ParentLocationName,PostalCode,UnivCity_recontact_location from biobank_analytics.pmbb_saliva.departments order by UnivCity_recontact_location desc"              
        cursor.execute(query)
                
        # Fetch all results
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        return render_template('locations.html', rows=rows)
    except Exception as e:
        return f"Error: {str(e)}"

@app.route('/location/<location_id>', methods=['GET', 'POST'])
def get_location_appointments(location_id):
    """Jawn for the location appointments"""
    try:
        connection = get_databricks_connection()
        cursor = connection.cursor()
        where = f" DepartmentEpicId = {location_id}"

        date_from = request.args.get('date_from', '').strip()
        date_to = request.args.get('date_to', '').strip()

        date_conditions = ""
        date_params = []
        if date_from:
            date_conditions += " AND A.AppointmentDate >= ?"
            date_params.append(date_from)
        if date_to:
            date_conditions += " AND A.AppointmentDate <= ?"
            date_params.append(date_to)

        query = f"SELECT A.EMPI, A.Patient_email, A.Patient_name, A.AppointmentConfirmationStatus, A.AppointmentDate, A.AppointemntTime FROM biobank_analytics.pmbb_saliva.upcoming_appointments_for_saliva A LEFT JOIN biobank_analytics.pmbb_saliva.scheduled_collection B ON A.EMPI = B.EMPI WHERE A.DepartmentEpicId = {location_id} AND (B.collection_id IS NULL OR B.outcome <> true){date_conditions}"
        cursor.execute(query, date_params)
        rows = cursor.fetchall()

        location_query = f"select * from biobank_analytics.pmbb_saliva.departments where {where} LIMIT 1"
        cursor.execute(location_query, (location_id))
        location_information = cursor.fetchall()

        scheduled_collections_query = f"Select * from biobank_analytics.pmbb_saliva.scheduled_collection where {where}"
        cursor.execute(scheduled_collections_query,(location_id))
        scheduled_collections_rows = cursor.fetchall()

        cursor.close()
        connection.close()

        return render_template('location.html', rows=rows, location_info=location_information, loc_id=location_id, scheduled_collections=scheduled_collections_rows, date_from=date_from, date_to=date_to)
    except Exception as e:
        return f"Error: {str(e)}"
    
@app.route('/location_export/<location_id>', methods=['GET', 'POST'])
def get_location_appointments_export(location_id):
    """Jawn for the location appointments"""
    try:
        connection = get_databricks_connection()
        cursor = connection.cursor()
        where = f" DepartmentEpicId = {location_id}"

        query = f"SELECT  A.EMPI, A.Patient_email, A.Patient_name, A.AppointmentConfirmationStatus, A.AppointmentDate, A.AppointemntTime FROM biobank_analytics.pmbb_saliva.  upcoming_appointments_for_saliva A LEFT JOIN biobank_analytics.pmbb_saliva.scheduled_collection B ON A.EMPI = B.EMPI WHERE A.DepartmentEpicId = {location_id} AND (B.collection_id IS NULL OR B.outcome <> true);"           
        cursor.execute(query, (location_id))
                
        # Fetch all results
        rows = cursor.fetchall()
        

        location_query = f"select * from biobank_analytics.pmbb_saliva.departments where {where} LIMIT 1"
        cursor.execute(location_query, (location_id))
        location_information = cursor.fetchall()

        cursor.close()
        connection.close()

        return render_template('location_export.html', rows=rows,location_info=location_information)
    except Exception as e:
        return f"Error: {str(e)}"
    


@app.route('/contact/<person_id>', methods=['GET', 'POST'])
def get_person_contacts(person_id):
    """Jawn for the location appointments"""

    connection = get_databricks_connection()
    cursor = connection.cursor()

    if request.method == 'POST':
        try:
            user_email = get_current_databricks_user_email()
            person_id = int(request.form['empi'])
            
            notes = request.form['notes']
            contact_type = int(request.form['contact_type'])
            study = int(request.form['study'])
            current_time_python = datetime.now()

            # Parameterized INSERT prevents SQL injection [web:26]
            table_name = "biobank_analytics.pmbb_saliva.recontact"
            sql_insert = f"INSERT INTO {table_name} (empi_id,contact_type,project,inserted,notes,staff_member) VALUES (?,?,?,?,?,?)"
            cursor = connection.cursor()
            cursor.execute(sql_insert,(person_id,contact_type,study,current_time_python,notes,user_email))
            connection.commit()
            connection.close()
            return redirect(url_for('person_details',person_id=person_id))  # Redirect to success page
        except Exception as e:
                return f"Error: {str(e)}"
    
    if request.method == 'GET':
        try:
            
            
            query = f"select * from biobank_analytics.pmbb_saliva.substudies"              
            cursor.execute(query)
            # Fetch all results
            rows_studies = cursor.fetchall()
            cursor.close()
            connection.close()
            
            return render_template('add_contact.html', pid=person_id,studies=rows_studies)

        except Exception as e:
            return f"Error: {str(e)}"

@app.route('/studies', methods=['GET', 'POST'])
def get_studies():
    """Jawn for the studies"""
    try:
    
        connection = get_databricks_connection()
        cursor = connection.cursor()

        query = "select substudy_id,substudy_name from biobank_analytics.pmbb_saliva.substudies"              
        cursor.execute(query)
                
        # Fetch all results
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        return render_template('studies.html', rows=rows)
    except Exception as e:
        return f"Error: {str(e)}"
    
@app.route('/study/<study_id>', methods=['GET'])
def get_study(study_id):
    """Jawn for the studies"""
    try:
        connection = get_databricks_connection()
        cursor = connection.cursor()

        query = "SELECT upcoming_appointments_for_saliva.EMPI, Patient_name,AppointmentInstant,AppointmentConfirmationStatus,DepartmentEpicId,Appointment_Location FROM biobank_analytics.pmbb_saliva.upcoming_appointments_for_saliva LEFT JOIN biobank_analytics.pmbb_saliva.substudy_cohorts ON biobank_analytics.pmbb_saliva.upcoming_appointments_for_saliva.EMPI = biobank_analytics.pmbb_saliva.substudy_cohorts.EMPI WHERE biobank_analytics.pmbb_saliva.substudy_cohorts.substudy_id = ? ORDER BY AppointmentInstant"
                 
        cursor.execute(query,[study_id])          
                        
        # Fetch all results
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        return render_template('study.html', rows=rows,sid=study_id)
    except Exception as e:
        return f"Error: {str(e)}"
    
@app.route('/schedule/<appointment_id>', methods=['GET', 'POST'])
def schedule_appointment(appointment_id):
    """Jawn for the studies"""
    try:
        connection = get_databricks_connection()
        cursor = connection.cursor()

        query = "SELECT * FROM biobank_analytics.pmbb_saliva.upcoming_appointments_for_saliva WHERE EncounterEpicCsn = ? LIMIT 1"              
        cursor.execute(query,[appointment_id])   
                
        # Fetch all results
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        return render_template('schedule.html', rows=rows)
    except Exception as e:
        return f"Error: {str(e)}"
    
@app.route('/schedule_me/<appointment_id>', methods=['GET', 'POST'])
def schedule_appointment_final(appointment_id):
    """Jawn for the studies"""
    try:
        connection = get_databricks_connection()
        cursor = connection.cursor()

        query = "SELECT * FROM biobank_analytics.pmbb_saliva.upcoming_appointments_for_saliva WHERE EncounterEpicCsn = ? LIMIT 1"              
        cursor.execute(query,[appointment_id])   
                
        # Fetch all results
        rows = cursor.fetchall()
        
        user_email = get_current_databricks_user_email()
        EMPI = rows[0]['EMPI']
        DepartmentEpicId = rows[0]['DepartmentEpicId']
        appointment_id = rows[0]['EncounterEpicCsn']
        location_name = rows[0]['Appointment_Location']
        timestamp_me = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        appointment_date = rows[0]['AppointmentDate']
        appointment_time = rows[0]['AppointemntTime']
        patient_name = rows[0]['Patient_name']
        
        sql_insert = f"INSERT INTO biobank_analytics.pmbb_saliva.scheduled_collection (appointment_id,EMPI,DepartmentEpicId,staff_member,created_at,location_name,appointment_date,appointment_time,patient_name) VALUES (?,?,?,?,?,?,?,?,?)"
        cursor = connection.cursor()
        cursor.execute(sql_insert,(appointment_id,EMPI,DepartmentEpicId,user_email,timestamp_me,location_name,appointment_date,appointment_time,patient_name))
        connection.commit()
        
        
        connection.close()

       # return render_template('schedule.html', rows=rows)
        return redirect(url_for("person_details", person_id=rows[0]['EMPI']))
    except Exception as e:
        return f"Error: {str(e)}"

@app.route('/completed_collections', methods=['GET'])
def completed_collections():
    """All collected samples"""
    try:
        connection = get_databricks_connection()
        cursor = connection.cursor()
        query = """
            SELECT cs.collection_id, cs.EMPI, cs.saliva_kit_id, cs.collected_by, cs.location_id, cs.created_date,
                   sc.patient_name, sc.location_name
            FROM biobank_analytics.pmbb_saliva.collected_sample cs
            LEFT JOIN biobank_analytics.pmbb_saliva.scheduled_collection sc ON cs.collection_id = sc.collection_id
            ORDER BY cs.created_date DESC
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        return render_template('completed_collections.html', rows=rows)
    except Exception as e:
        return f"Error: {str(e)}"

@app.route('/upcoming_collections', methods=['GET', 'POST'])
def upcoming_collections():
    """Jawn for the studies"""
    try:
        connection = get_databricks_connection()
        cursor = connection.cursor()

        date_from = request.args.get('date_from', '').strip()
        date_to = request.args.get('date_to', '').strip()

        date_conditions = ""
        date_params = []
        if date_from:
            date_conditions += " AND t1.appointment_date >= ?"
            date_params.append(date_from)
        if date_to:
            date_conditions += " AND t1.appointment_date <= ?"
            date_params.append(date_to)

        query = f"SELECT t1.*, t2.saliva_kit_id FROM biobank_analytics.pmbb_saliva.scheduled_collection AS t1 LEFT JOIN biobank_analytics.pmbb_saliva.collected_sample AS t2 ON t1.collection_id = t2.collection_id WHERE t1.outcome IS NULL{date_conditions} ORDER BY t1.appointment_date, t1.appointment_time"
        cursor.execute(query, date_params)

        rows = cursor.fetchall()
        connection.close()

        return render_template('upcoming_collections.html', rows=rows, date_from=date_from, date_to=date_to)
    except Exception as e:
        return f"Error: {str(e)}"
    
@app.route('/participants', methods=['GET', 'POST'])
def participants():
    """Jawn for the studies"""
    try:
        rows1 = []
        has_params = bool(request.args) 
        if has_params:
            empi = request.form.get('empi', '').strip()
            patient_name = request.form.get('patient_name', '').strip()
            hup_mrn = request.form.get('hup_mrn', '').strip()

            connection = get_databricks_connection()
            cursor = connection.cursor()
           
            query = "SELECT DISTINCT EMPI, Patient_name, HUP_MRN FROM biobank_analytics.pmbb_saliva.upcoming_appointments_for_saliva WHERE EMPI LIKE %s LIMIT 100"
            cursor.execute(query, (f"%{empi}%",))

            rows1 = cursor.fetchall()
            connection.close()
           
        return render_template('participants.html', rows=rows1)
    
    except Exception as e:
        return f"Error: {str(e)}"

@app.route('/donotcontact/<person_id>', methods=['GET', 'POST'])
def do_not_contact(person_id):
    """Jawn for the studies"""
    try:
        connection = get_databricks_connection()
        cursor = connection.cursor()
        query = "select * from biobank_analytics.pmbb_saliva.upcoming_appointments_for_saliva where EMPI = ? LIMIT 1"     
        cursor.execute(query,[person_id])    
                
        # Fetch all results
        rows = cursor.fetchall()

        contact_query = "select * from biobank_analytics.pmbb_saliva.recontact where empi_id = ?";
        cursor.execute(contact_query,[person_id])    
        contact_rows = cursor.fetchall()

        connection.close()
      
        return render_template('donotcontact.html', rows=rows, contact_rows=contact_rows)
    except Exception as e:
        return f"Error: {str(e)}"
    
@app.route('/collect_me/<collection_id>', methods=['GET', 'POST'])
def collect_me(collection_id):
    """COLLECT SAMPLE"""

    connection = get_databricks_connection()
    cursor = connection.cursor()

    if request.method == 'GET':
        try:
            query = "select * from biobank_analytics.pmbb_saliva.scheduled_collection where collection_id = ? LIMIT 1"              
            cursor.execute(query,[collection_id])   
                    
            # Fetch all results
            rows = cursor.fetchall()
            cursor.close()
            connection.close()

            return render_template('collect_sample.html', rows=rows, cid=collection_id)
        except Exception as e:
            return f"Error: {str(e)}"
    if request.method == 'POST':
        try:
            user_email = get_current_databricks_user_email()
            coll_id = int(request.form['collection_id'])
            saliva_tube_kit_id = int(request.form['saliva_tube_kit_id'])
            sharpie = int(request.form['sharpie'])
            location_id = int(request.form['location_id'])
            empi = request.form['empi']

            current_time_python = datetime.now()

            # Parameterized INSERT prevents SQL injection [web:26]
            table_name = "biobank_analytics.pmbb_saliva.collected_sample"
            sql_insert = f"INSERT INTO {table_name} (collection_id,EMPI,saliva_kit_id,collected_by,location_id,created_date,sharpie) VALUES (?,?,?,?,?,?,?);"
            cursor = connection.cursor()
            cursor.execute(sql_insert,(coll_id,empi,saliva_tube_kit_id,user_email,location_id,current_time_python,sharpie))
            connection.commit()

            update_table = "biobank_analytics.pmbb_saliva.scheduled_collection"
            update_collection_event = f"UPDATE {update_table} SET outcome = TRUE WHERE collection_id = ?;"
            cursor.execute(update_collection_event,(collection_id))

            connection.close()
            return redirect(url_for('person_details',person_id=empi))  # Redirect to success page
        except Exception as e:
                return f"Error: {str(e)}"

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)