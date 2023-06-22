from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import csv
import pandas as pd
from io import StringIO 
from datetime import datetime
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Database connection setup
def create_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="SuryaMasterbatches",
        user="postgres",
        password="123456"
    )
    return conn


@app.route('/api/retrieve', methods=['GET'])
def retrieve_values():
    try:
        conn = create_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM raw_material")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        result = [dict(zip(columns, row)) for row in rows]
        return jsonify(result)
    except (Exception, psycopg2.Error) as error:
        return jsonify({'error': str(error)})
    finally:
        if conn:
            cursor.close()
            conn.close()


@app.route('/api/update', methods=['POST'])
def update_values():
    try:
        conn = create_connection()
        cursor = conn.cursor()
        csv_file = request.files['file']
        csv_data = csv_file.read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_data))

        # Iterate over each row in the CSV file
        update_query = "UPDATE raw_material SET rawmaterialprice = %(price)s WHERE rawmaterialid = %(id)s"

        # Create a list of dictionaries containing the parameter values for each row
        parameters = []
        for _, row in df.iterrows():
            row_data = {'price': row[2], 'id': row[0]}  # Assuming rawmaterialid is in column 0 and rawmaterialprice is in column 2
            parameters.append(row_data)

        # Execute the bulk update query
        cursor.executemany(update_query, parameters)
        conn.commit()

        # Commit the changes to the database
       
        return jsonify({'message': 'Update successful'})
    except (Exception, psycopg2.Error) as error:
        return jsonify({'error': str(error)})
    finally:
        if conn:
            cursor.close()
            conn.close()


@app.route('/api/clients', methods=['POST'])#client update
def add_client():
    data = request.json
    client_id = data['id']
    client_name = data['name']
    client_details = data['details']
    interested_product = data['interestedProduct']
    creation_date = data['dateCreated']

    conn = create_connection()
    if conn is None:
        return jsonify({'error': 'Failed to connect to the database.'}), 500

    try:
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO client_detail (client_id, client_name, client_detail, interested_product,creation_date)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (client_id, client_name,client_details, creation_date, interested_product))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({'message': 'Client added successfully!'}), 200
    except (Exception, psycopg2.Error) as error:
        print('Error while inserting data into PostgreSQL:', error)
        conn.rollback()
        conn.close()
        return jsonify({'error': 'Failed to add client.'}), 500


@app.route('/api/retrieve/products', methods=['GET'])#Product details
def retrieve_prod_values():
    try:
        conn = create_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT product_id,product_name,product_category,product_subcat,product_sp,product_description FROM product")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        result = [dict(zip(columns, row)) for row in rows]
        return jsonify(result)
    except (Exception, psycopg2.Error) as error:
        return jsonify({'error': str(error)})
    finally:
        if conn:
            cursor.close()
            conn.close()

@app.route('/api/update/product', methods=['POST'])
def update_product_values():
    try:
        conn = create_connection()
        cursor = conn.cursor()
        csv_file = request.files['file']
        csv_data = csv_file.read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_data))
        print(df)

        # Add product_creationdate column to DataFrame
        df['product_creationdate'] = datetime.now()

        # Convert DataFrame to list of dictionaries
        parameters = df.to_dict('records')

        # Define update query with named parameters
        update_query = """
            UPDATE product 
            SET 
                product_name = %(product_name)s, 
                product_category = %(product_category)s, 
                product_subcat = %(product_subcat)s, 
                product_sp = %(product_sp)s, 
                product_description = %(product_description)s,
                product_creationdate = %(product_creationdate)s
            WHERE product_id = %(product_id)s
        """

        # Execute the bulk update query
        cursor.executemany(update_query, parameters)
        conn.commit()

        # Commit the changes to the database
        return jsonify({'message': 'Update successful'})
    except (Exception, psycopg2.Error) as error:
        return jsonify({'error': str(error)})
    finally:
        if conn:
            cursor.close()
            conn.close()
@app.route('/api/products/<product_id>', methods=['GET'])
def get_product_data(product_id):
    try:
        # Connect to the MySQL database
        conn = create_connection()

        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()
        print(product_id)
        # Execute the SQL query
        query = """
            SELECT pt.product_name,rmmt.rawmaterialid, rm.rawmaterialname, rm.rawmaterialprice,rmmt.qtybyformula
            FROM productrawmaterialmapping rmmt
            JOIN raw_material rm ON rmmt.rawmaterialid = rm.rawmaterialid
            JOIN product pt ON rmmt.product_id = pt.product_id
            WHERE pt.product_id = %s;
        """
        cursor.execute(query,(product_id.upper(),))

        # Fetch the query results
        result = cursor.fetchall()
        print(result)

        # Format the query result
        product_data = []
        for row in result:
            product_data.append({
                'product_name': row[0],
                'rawmaterialid': row[1],
                'rawmaterialname': row[2],
                'rawmaterialprice': row[3],
                'qtybyformula': row[4]
            })

        # Close the cursor and database connection
        cursor.close()
        conn.close()

        # Return the product data as JSON response
        return jsonify(product_data)

    except Exception as e:
        # Handle any exceptions that occur during the database connection or query execution
        print('Error fetching product data:', e)
        return jsonify({'error': 'An error occurred while fetching product data.'}), 500



if __name__ == '__main__':
    app.run(debug=True)
