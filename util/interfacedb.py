import csv
import os
import stat

from influxdb import DataFrameClient, InfluxDBClient
import pandas as pd
import logging

logger = logging.getLogger('daq.{0}'.format(__name__))

def get_access(source):
    '''Get access to data source.

    Parameters
    ----------
    source : string
        Name of data source.

    Returns
    -------
    args : list
        Line of access file corresponding to data source name.

    '''
    # Get local directory
    path = os.path.dirname(os.path.abspath(__file__))
    # Get access data
    try:
        # Check permissions
        # Python2: &0777, In Ubuntu version, is '0600', was revised to '0666' in the windows version
        # Python3: &777, '0o400'
        if oct(os.stat(path+'/access.config')[stat.ST_MODE] & 777) != '0o400':
            print('Accessing file with wrong permissions.')
            raise IOError
        # Open access file
        with open(path+'/access.config', 'r') as f:
            reader = csv.reader(f)
            # Search for correct line
            for line in reader:
                # Assign args
                if bytes.fromhex(line[0]).decode('utf-8') == source:
                    args = line
                    break
                else:
                    args = None
    # Handle errors
    except IOError:
        raise IOError('Access file not found or has incorrect permissions.  Access denied.')
    if not args:
        raise ValueError('Data source name not found in access file.  Access denied.')

    user_name = bytes.fromhex(args[1]).decode('utf-8')
    password = bytes.fromhex(args[2]).decode('utf-8')
    # print(password)
    return user_name, password

def setup_lbnl(databaseName='building59', style=None):
    '''Setup the connection to the LBNL-hosted database holding demo data.

    Parameters
    ----------
    databaseName : str
        'wifi' to setup connection to the Lab Wifi database
        'building59' to setup connection to Building 59 database.
        Default is building59.    
    
    style : str
        'forecasts' to setup connection to database for forecasts.
        'generic' to setup connection to database for generic data.
        None to setup connection to database based on DataFrameClient.
        Default is None.

    Returns
    -------
    db : database object
        Database object configured to interface with LBNL-hosted database.
    dbname : str
        Name of database with data.

    '''
    # Setup parameters

    if databaseName=='wifi':
        host='eln-data-store.lbl.gov'
        port=9003
        dbname = 'wifi'
        username, password = get_access('wifi')
        username = username
        password = password
    elif databaseName=='building59':
        host='mpcdata.lbl.gov'
        port=8086
        dbname = 'dhblum'
        # Get access
        username, password = get_access('IDBC')
        username = username
        password = password
    else:
        raise NameError('Database not found')

    # Initialize database
    if style is None:
        db = database(host, port, 'dhblum', username, password, ssl=True, verify_ssl=True)
    elif style is 'forecasts':
        db = database_forecast(host, port, 'dhblum', username, password, ssl=True, verify_ssl=True)
    elif style is 'generic':
        db = database_generic(host, port, 'dhblum', username, password, ssl=True, verify_ssl=True)
    else:
        raise ValueError('Unknown database interface style {0}.'.format(style))

    #print(username, password)
    return db, dbname

class database(object):
    '''Class to facilitate writing and reading from influxdb database.

    Parameters
    ----------
    host : string
        Hostname to connect to InfluxDB. Suggested ‘localhost’.
    port : int
        Port to connect to InfluxDB. Suggested 8086.
    dbname : string
        Name of database.
    user : string
        Username.
    password : string
        Password for user.
    ssl : bool, optional
        use https instead of http to connect to InfluxDB, defaults to False
    verify_ssl : bool, optional
        verify SSL certificates for HTTPS requests, defaults to False

    Attributes
    ----------
    client : influxdb DataFrameClient
        Client interface to database.

    '''

    def __init__(self, host, port, dbname, user, password, ssl=False, verify_ssl=False ):
        '''Constructor of database class for interfacing with influxdb.

        '''

        # Create client
        self.client = DataFrameClient(host, port, user, password, database=dbname, ssl=ssl, verify_ssl=verify_ssl)

    def create_database(self, dbname):
        '''Create new database.

        Parameters
        ----------
        dbname : string
            Name of database.

        Returns
        -------
        flag : int
            1 if database created successfully.
            -1 if database name already found.

        '''

        # Check if database exists and create if not
        db_list = self.client.get_list_database()
        exist = 0
        for db in db_list:
            if dbname in db['name']:
                exist = 1
                break
            else:
                exist = 0
        if exist:
            logger.info('Database {0} found already.'.format(dbname))
            flag = -1
        else:
            logger.info('Creating database {0}.'.format(dbname))
            self.client.create_database(dbname)
            flag = 1

        return flag

    def write_data(self, df, dbname, compatible_names=True):
        '''Write dataframe to database.

        Parameters
        ----------
        df : pandas DataFrame
            DataFrame of data to write to database.
        dbname : string
            Name of database to write data to.
        compatible_names : boolean, default True
            Mark True to convert dataframe column names to be compatible with
            influxdb measurement names as follows:
            ``name.replace('-','_').replace('#','').replace('/','_')``.

        Returns
        -------
        flag : int
            1 if data written successfully.
            -1 if error messages should be checked.

        '''

        # Initialize
        flag = 1
        # Convert keys to influxdb compatible if requested
        if compatible_names:
            columns = {};
            for key in df.columns.values:
                columns[key] = key.replace('-','_').replace('#','').replace('/','_').replace(' ', '_')
            df.rename(columns=columns, inplace=True)
        # Store data
        for key in df.columns.values:
            try:
                self.client.write_points(df[key].to_frame().dropna(), key, database=dbname)
                logger.info('Data for {0} written to database {1}.'.format(key,dbname))
                if flag == -1:
                    flag = -1
            except TypeError:
                logger.warning('{0} not able to be stored in database.  Contains all NaN.'.format(key))
                flag = -1

        return flag

    def get_data(self, key, dbname, start_time, final_time, compatible_names=True):
        '''Get data from database.

        Parameters
        ----------
        key : string
            Name of measurement to get data for.
        dbname : string
            Name of database to get data from.
        start_time : string
            Start time of data collection in UTC.
        final_time : string
            Final time of data collection in UTC.
        compatible_names : boolean, default True
            Mark True to convert key to be compatible with
            influxdb measurement names as follows:
            ``name.replace('-','_').replace('#','').replace('/','_')``.
            DataFrame column names will be returned as original key.

        Returns
        -------
        df : pandas DataFrame
            DataFrame with column name as key and index as timestamp in UTC.

        '''

        # Convert times to utc and rfc3339
        start_time_db = pd.to_datetime(start_time).tz_localize('UTC').strftime('%Y-%m-%d %H:%M:%S');
        final_time_db = pd.to_datetime(final_time).tz_localize('UTC').strftime('%Y-%m-%d %H:%M:%S');
        # Make compatible if requested
        if compatible_names:
            key_old = key;
            key = key.replace('-','_').replace('#','').replace('/','_')
        # Get data
        query = "SELECT * FROM {0} WHERE time >= '{1}' AND time <= '{2}'".format(key, start_time_db, final_time_db)
        res = self.client.query(query, database=dbname)
        try:
            df = res[key];
            # Change key name back to non-compatible if requested
            if compatible_names:
                columns = {key : key_old}
                df.rename(columns=columns, inplace=True)
            logger.info('Data for {0} retrieved from database {1}.'.format(key,dbname))
        except KeyError:
            logger.warning('Data for {0} not found in database.  Check that the key and time interval are correct and that there is data during the time interval.'.format(key))
            df = None

        return df

    def drop_measurement(self, key, dbname, compatible_names=True):
        '''Drop an entire measurement from database.

        Parameters
        ----------
        key : string
            Name of measurement to drop from database.
        dbname : string
            Name of database to drop from.
        compatible_names : boolean, default True
            Mark True to convert key to be compatible with
            influxdb measurement names as follows:
            ``name.replace('-','_').replace('#','').replace('/','_')``.
            DataFrame column names will be returned as original key.

        '''

        # Make compatible if requested
        if compatible_names:
            key = key.replace('-','_').replace('#','').replace('/','_')
        # Drop measurement
        query = "DROP MEASUREMENT {0}".format(key)
        res = self.client.query(query, database=dbname)

    def get_measurements_list(self, dbname):
        '''Get data from database.

        Parameters
        ----------
        dbname : string
            Name of database from which to get list of measurements.

        Returns
        -------
        measurements_list : list of strings
            List of measurements in database dbname.

        '''

        # Get data
        query = "SHOW MEASUREMENTS"
        res = self.client.query(query, database=dbname)
        # Parse data into list
        measurements_list = []
        for d in res['measurements']:
            measurements_list.append(d['name'])

        return measurements_list

class database_forecast(database):
    '''Class to facilitate writing and reading from influxdb database for forecasts.

    Sub class of database with redefined write_data and get_data methods.

    Parameters
    ----------
    host : string
        Hostname to connect to InfluxDB. Suggested ‘localhost’.
    port : int
        Port to connect to InfluxDB. Suggested 8086.
    dbname : string
        Name of database.
    user : string
        Username.
    password : string
        Password for user.
    ssl : bool, optional
        use https instead of http to connect to InfluxDB, defaults to False
    verify_ssl : bool, optional
        verify SSL certificates for HTTPS requests, defaults to False

    Attributes
    ----------
    client : influxdb InfluxDBClient
        Client interface to database.

    '''

    def __init__(self, host, port, dbname, user, password, ssl=False, verify_ssl=False):
        '''Constructor of database class for interfacing with influxdb.

        '''

        # Create client
        self.client = InfluxDBClient(host, port, user, password, database=dbname, ssl=ssl, verify_ssl=verify_ssl)

    def write_data(self, df, dbname, time = None, compatible_names=True):
        '''Write hourly forecast dataframe to database.

        Forecasts begin in the next hour from current.  Current time is used
        for timestamp in database.

        Parameters
        ----------
        df : pandas DataFrame
            DataFrame of data to write to database.  Index should be
            datetimeindex in UTC.
        dbname : string
            Name of database to write data to.
        time : str, default None
            Time in UTC for which to store forecast in "%Y-%m-%d %H:%M:%S".
            If None, the forecast will be stored for the hour before the first
            index element in df.
        compatible_names : boolean, default True
            Mark True to convert dataframe column names to be compatible with
            influxdb measurement names as follows:
            ``name.replace('-','_').replace('#','').replace('/','_')``.

        '''
        import cleaning
        # Detect sample rate and make sure it is constant
        likely_sample_rate, num_sample_rates = cleaning.detect_likely_sample_rate(df)
        if num_sample_rates > 1:
            raise ValueError('The sample rate of the forecast is not constant.  Cannot write to database.')
        # Convert time to utc and rfc3339
        if not time:
            time_db = (df.index[0]-pd.Timedelta(hours=1)).strftime('%Y-%m-%d %H:00:00');
            start_time = time_db
        else:
            time_db = time
            start_time = (df.index[0]-pd.Timedelta(seconds=likely_sample_rate)).strftime('%Y-%m-%d %H:%M:%S');
        # Convert keys to influxdb compatible if requested
        if compatible_names:
            columns = {};
            for key in df.columns.values:
                columns[key] = key.replace('-','_').replace('#','').replace('/','_').replace(' ', '_')
            df.rename(columns=columns, inplace=True)
        # Build message
        message = []
        for key in df.columns.values:
            json_body = {'measurement':key, 'time':time_db, 'fields':dict()}
            i = 1
            for value in df[key].values:
                json_body['fields']['{0}'.format(i)] = value
                i = i + 1
            json_body['fields']['sample_rate'] = likely_sample_rate
            json_body['fields']['start_time'] = start_time
            message.append(json_body)
        # Store data
        self.client.write_points(message, database=dbname)
        logger.info('Data for {0} written to database {1}.'.format(df.columns.values,dbname))

    def get_data(self, key, dbname, time, compatible_names=True):
        '''Get forecast data from database.

        Parameters
        ----------
        key : string
            Name of measurement to get data for.
        dbname : string
            Name of database to get data from.
        time : string
            Time stamp of data forecast collection in UTC.
        compatible_names : boolean, default True
            Mark True to convert key to be compatible with
            influxdb measurement names as follows:
            ``name.replace('-','_').replace('#','').replace('/','_')``.
            DataFrame column names will be returned as original key.

        Returns
        -------
        df : pandas DataFrame
            DataFrame with column name as key and index as timestamp in UTC.
            Returns empty DataFrame if no data found.

        '''

        # Convert time to utc and rfc3339
        time_db = pd.to_datetime(time).tz_localize('UTC').strftime('%Y-%m-%d %H:%M:%S');
        # Make compatible if requested
        if compatible_names:
            key_old = key;
            key = key.replace('-','_').replace('#','').replace('/','_')
        # Get data
        query = "SELECT * FROM {0} WHERE time = '{1}'".format(key, time_db)
        res = self.client.query(query, database=dbname)
        logger.info('Data for {0} retrieved from database {1}.'.format(key, dbname))
        points = res.get_points()
        # Convert to dataframe
        df_points = pd.DataFrame(points).transpose()
        if not df_points.empty:
            df = pd.DataFrame()
            try:
                sample_rate = int(df_points.loc['sample_rate',0])
            except KeyError:
                logger.info('sample_rate not found.  Using 3600 seconds.')
                sample_rate = 3600
            except TypeError:
                logger.info('sample_rate found, but None.  Using 3600 seconds.')
                sample_rate = 3600
            try:
                start_time = pd.to_datetime(df_points.loc['start_time',0])
                if not start_time:
                    logger.info('start_time found, but None. Using database measurement time.')
                    start_time = pd.to_datetime(time)
            except KeyError:
                logger.info('start_time not found. Using database measurement time.')
                start_time = pd.to_datetime(time)
            for i in df_points.index.values:
                try:
                    t = start_time+pd.Timedelta(seconds=float(i)*sample_rate)
                    df.loc[t,key] = df_points.loc[i,0]
                except:
                    pass
            # Change key name back to non-compatible if requested
            if compatible_names:
                columns = {key : key_old}
                df.rename(columns=columns, inplace=True)

            # Sort by index
            df = df.sort_index()
            # Make UTC time
            df = df.tz_localize('UTC')
        else:
            df = pd.DataFrame()
            logger.warning('Data retrieved is empty.')

        return df

class database_generic(database):
    '''Class to facilitate writing and reading from influxdb database for generic measurements.

    Sub class of database with redefined write_data and get_data methods.

    Parameters
    ----------
    host : string
        Hostname to connect to InfluxDB. Suggested ‘localhost’.
    port : int
        Port to connect to InfluxDB. Suggested 8086.
    dbname : string
        Name of database.
    user : string
        Username.
    password : string
        Password for user.
    ssl : bool, optional
        use https instead of http to connect to InfluxDB, defaults to False
    verify_ssl : bool, optional
        verify SSL certificates for HTTPS requests, defaults to False

    Attributes
    ----------
    client : influxdb InfluxDBClient
        Client interface to database.

    '''

    def __init__(self, host, port, dbname, user, password, ssl=False, verify_ssl=False):
        '''Constructor of database class for interfacing with influxdb.

        '''

        # Create client
        self.client = InfluxDBClient(host, port, user, password, database=dbname, ssl=ssl, verify_ssl=verify_ssl)

    def write_data(self, name, dbname, time, message):
        '''Write generic data to influexdb.

        Parameters
        ----------
        name : str
            Name of measurement.
        dbname: str
            Name of database table to write to.
        time : str
            Timestamp of measurement in UTC.
        message : dict
            Dictionary of field:value pairs.

        '''

        # Convert times to utc and rfc3339
        time_db = pd.to_datetime(time).tz_localize('UTC').strftime('%Y-%m-%d %H:%M:%S');
        # Build message
        json_body = {'measurement':name, 'time':time_db, 'fields':dict()}
        for field in message.keys():
            json_body['fields'][field] = message[field]
        # Store data
        json_body = [json_body]
        self.client.write_points(json_body, database=dbname)
        logger.info('Data for {0} written to database {1}.'.format(name,dbname))

    def get_data(self, name, dbname, time):
        '''Get hourly forecast data from database.

        Parameters
        ----------
        name : str
            Name of measurement.
        dbname: str
            Name of database table to write to.
        time : str
            Timestamp of measurement in UTC.

        Returns
        -------
        data : dict
            Dictionary of field:value pairs.
            None is returned if no data found.

        '''

        # Convert times to utc and rfc3339
        time_db = pd.to_datetime(time).tz_localize('UTC').strftime('%Y-%m-%d %H:%M:%S');
        # Get data
        query = "SELECT * FROM {0} WHERE time = '{1}'".format(name, time_db)
        res = self.client.query(query, database=dbname)
        # Check data exists
        if 'series' in res.raw.keys():
            logger.info('Data for {0} retrieved from database {1}.'.format(name, dbname))
            message = res.raw['series'][0]
            # Format data
            data = dict()
            data['name'] = message['name']
            for i,key in enumerate(message['columns']):
                data[key] = message['values'][0][i]
        else:
            logger.warning('Could not find data for {0} at {1}.  Check that the key and time are correct and that data exists.'.format(name, time_db))
            data = None

        return data

    def get_data_all_raw(self, key, dbname, compatible_names=True):
        '''Get data from database.()

        Parameters
        ----------
        key : string
            Name of measurement to get data for.
        dbname : string
            Name of database to get data from.
        compatible_names : boolean, default True
            Mark True to convert key to be compatible with
            influxdb measurement names as follows:
            ``name.replace('-','_').replace('#','').replace('/','_')``.
            DataFrame column names will be returned as original key.

        Returns
        -------

        '''

        # Make compatible if requested
        if compatible_names:
            key_old = key;
            key = key.replace('-','_').replace('#','').replace('/','_')
        # Get data
        query = "SELECT * FROM {0}".format(key)
        res = self.client.query(query, database=dbname)

        return res
