import requests
import re
import os
import streamlit as st
import plotly.express as px
import pandas as pd
import threading
from dotenv import dotenv_values

from db_handler import DbHandler


def main():
    # when starting, open DB connection

    db_handler = DbHandler()
    if db_handler.conn is None:
        db_handler.init_db()

    access_log_path = '/var/log/nginx/access.log'
    # error_log_path = '/var/log/nginx/error.log'

    access_log_pattern = re.compile(
        r'''^(?P<remote_addr>\S+) (?P<remote_user>\S+) \S+ \[(?P<time_local>[^]]+)\] "(?P<request>[^"]*)" (?P<status>\d+) (?P<body_bytes_sent>\d+) "(?P<http_referer>[^"]*)" "(?P<http_user_agent>[^"]*)"'''
    )
    access_log_lines = read_nginx_logs(access_log_path)
    # error_log_lines = read_nginx_logs(error_log_path)

    # for line in access_log_lines:
    #     print(line)

    access_df = create_log_df(access_log_lines, access_log_pattern)

    print(access_df)
    print(access_df.columns)

    filter_df(access_df, 'remote_addr', 10)
    print(count_unique_cols(access_df, 'remote_addr'))

    render_dashboard(access_df, db_handler)

    ips = filter_df(access_df, 'remote_addr', 3000)['remote_addr'].to_list()
    num_threads = 10

    ips_chunks = list(split_list_into_chunks(ips, len(ips) // num_threads))

    threads = []  # Initialize an empty list for threads
    for i in range(num_threads):
        thread = threading.Thread(target=get_ip_geolocation_bulk, args=(ips_chunks[i],))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()


def split_list_into_chunks(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


def read_nginx_logs(log_file):
    log_content = []
    try:
        with open(log_file, 'r') as file:
            log_content = file.readlines()
    except FileNotFoundError:
        print(f"File '{log_file}' not found. Trying to take locally saved logfile for testing from ~/nginx")
        try:
            new_file = os.path.join('./nginx', os.path.basename(log_file))
            print(f"Loading File: {new_file})")
            # Read a file from the local directory as a fallback
            with open(new_file, 'r') as local_file:
                log_content = local_file.readlines()
        except FileNotFoundError:
            print("Local file not found. No logs available.")

    return [line.rstrip('\n') for line in log_content]


def create_log_df(log_lines, pattern):
    # Extract log lines
    parsed_logs = []  # Initialize an empty list to store parsed log entries

    total_len = len(log_lines)
    matches_len = 0
    misses_len = 0

    # Parse each log line using the regex pattern
    for line in log_lines:
        match = re.match(pattern, line)
        if match:
            parsed_logs.append(match.groupdict())
            matches_len += 1
        else:
            misses_len += 1

    print(f"Total Rows: {total_len}, Matches: {matches_len}, Misses: {misses_len}")
    # Create a DataFrame from the parsed log entries
    df = pd.DataFrame(parsed_logs)

    # Convert timestamp column to datetime format
    df['time_local'] = pd.to_datetime(df['time_local'], format='%d/%b/%Y:%H:%M:%S %z', utc=True)
    return df


def get_ip_geolocation(ip_addr):
    ipgeolocation_url = f'https://api.ipgeolocation.io/ipgeo?apiKey={ipgeolocation_key}&ip={ip_addr}'
    response = requests.get(ipgeolocation_url, ip_addr)

    if response.status_code == 200:
        data = response.json()  # Get the JSON response data
        # print(data)
        return data
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return False


def get_ip_geolocation_bulk(ip_addrs):
    # not possible with ipgeolocation developer (free) subscription :(
    # API endpoint
    # ipgeolocation_url = f'https://api.ipgeolocation.io/ipgeo-bulk?apiKey={ipgeolocation_key}'
    #
    # # Request payload
    # payload = {"ips": ip_addrs}
    #
    # print(payload)
    # # Request headers
    # headers = {
    #     "Content-Type": "application/json",
    # }
    # # Adding the API key to the headers
    # params = {
    #     "apiKey": ipgeolocation_key
    # }
    #
    # # Making a POST request
    # response = requests.post(ipgeolocation_url, headers=headers, params=params, data=json.dumps(payload))
    #
    # # Handling the response
    # if response.status_code == 200:
    #     data = response.json()
    #     print(data)
    # else:
    #     print(f"Failed to fetch data. Status code: {response.status_code}")

    # here is the workaround ;)
    # yes, it will run in an own thread.

    thread_id = threading.current_thread().ident

    db_handler = DbHandler()
    if db_handler.conn is None:
        db_handler.init_db()

    for addr in ip_addrs:
        ip_loc = db_handler.retrieve_geolocation_by_ip(addr)
        if ip_loc:
            print(f'Thread ID: {thread_id} DB: {ip_loc}')
        elif not ip_loc:
            ip_loc = get_ip_geolocation(addr)
            if ip_loc:
                print(f'Thread ID: {thread_id} Fetch: {ip_loc}')
                db_handler.insert_geolocation_data(ip_loc)

    db_handler.close_db()


def filter_df(df, col, cnt=0):
    # Get the top x most common IP addresses
    top_most_common = df[col].value_counts().head(cnt) if cnt > 0 else df[col].value_counts()
    # Merge the counts back to the original DataFrame
    unique_top = pd.merge(df, top_most_common, on=col, how='inner') \
        .drop_duplicates(subset=col) \
        .sort_values(by='count', ascending=False)

    return unique_top


def count_unique_cols(df, col):
    return len(get_unique_cols(df, col))


def get_unique_cols(df, col):
    return df[col].drop_duplicates()


def render_dashboard(df, db_handler):
    ####################################################################################################################
    # Number of requests over time (line chart)
    df['time_local'] = pd.to_datetime(df['time_local'])

    # Grouping data by 'time_local' and counting the number of requests for each timestamp
    requests_over_time = df.groupby(pd.Grouper(key='time_local', freq='D')).size().reset_index(name='count')
    avg_requests = requests_over_time['count'].mean()

    # Creating an interactive line chart with Plotly
    fig_traffic_over_time = px.line(requests_over_time, x='time_local', y='count', title='Number of Requests Over Time')
    fig_traffic_over_time.update_xaxes(title='Date')
    fig_traffic_over_time.update_yaxes(title='Number of Requests')

    fig_traffic_over_time.add_hline(y=avg_requests, line_dash='dash', line_color='red',
                                    annotation_text=f'Average: {avg_requests:.2f}',
                                    annotation_position="bottom right")

    ####################################################################################################################
    # IP Location Map
    df_location = db_handler.retrieve_all_geolocations()

    # convert strings to numeric, so density_mapbox can read the values
    df_location['latitude'] = pd.to_numeric(df_location['latitude'], errors='coerce')
    df_location['longitude'] = pd.to_numeric(df_location['longitude'], errors='coerce')

    # Create a heatmap to represent point density
    fig_ip_locations = px.density_mapbox(df_location, lat='latitude', lon='longitude',
                                         radius=10, zoom=1, mapbox_style="carto-darkmatter",
                                         title='Estimated IP Address Locations')

    ####################################################################################################################
    # traffic pattern
    hourly_requests = df[['time_local', 'request']].copy()
    hourly_requests['time_local'] = pd.to_datetime(hourly_requests['time_local'])

    # Extract the hour component from the 'time_local' column
    hourly_requests['hour'] = hourly_requests['time_local'].dt.hour

    # Group by hour and count the number of requests for each hour
    hourly_requests = hourly_requests.groupby(['hour']).size().reset_index(name='requests')

    # Create a bar graph showing the number of requests for each hour
    fig_hourly_requests = px.bar(hourly_requests, x='hour', y='requests',
                                 title='Number of Requests per Hour',
                                 labels={'hour': 'Hour of the Day', 'requests': 'Number of Requests'})
    fig_hourly_requests.update_layout(xaxis=dict(tickvals=list(range(24)), ticktext=[str(i) + 'h' for i in range(24)]))

    ####################################################################################################################
    # Most Requested Endpoints
    # Get the count of each unique endpoint (http_referer)
    filtered_data = df[df['http_referer'] != '-']
    endpoint_counts = filtered_data['http_referer'].value_counts().reset_index()
    endpoint_counts.columns = ['Endpoint', 'Count']

    # Select the top 10 most requested endpoints (you can adjust the number as needed)
    top_endpoints = endpoint_counts.head(10)

    # Create a pie chart to visualize the distribution of the most requested endpoints
    fig_most_requested_endpoints = px.pie(top_endpoints, values='Count', names='Endpoint',
                                          title='Top 10 Most Requested Endpoints (ignoring endpoint / )')

    # fig_most_requested_endpoints.update_traces(textposition='inside', textinfo='percent')
    fig_most_requested_endpoints.update_layout(
        legend=dict(orientation="h"),
        height=600,  # Set the height of the chart
    )

    ####################################################################################################################
    # Top response codes
    # Get the count of each unique endpoint (http_referer)
    # filtered_data = df[df['status'] != '-']
    http_response_counts = df['status'].value_counts().reset_index()
    http_response_counts.columns = ['HTTP Status', 'Count']

    # Select the top 10 most requested endpoints (you can adjust the number as needed)
    top_http_responses = http_response_counts.head(5)

    # Create a pie chart to visualize the distribution of the most requested endpoints
    fig_top_http_responses = px.pie(top_http_responses, values='Count', names='HTTP Status',
                                    title='Top 5 Most HTTP Response Codes')

    ####################################################################################################################
    # Top user Agents
    # Counting occurrences of each user agent
    user_agents_count = df['http_user_agent'].value_counts().reset_index()
    user_agents_count.columns = ['User Agent', 'Count']

    # Creating a horizontal bar chart using Plotly Express
    # fig_user_agents = px.bar(user_agents_count.head(10), y='Count', x='User Agent',
    #              title='Top 10 Most Used User Agents')
    # fig_user_agents.update_layout(yaxis_title='Count', xaxis_title='User Agent')

    ####################################################################################################################
    # Top active IP Addresses
    # Count occurrences of each IP address
    ip_counts = df['remote_addr'].value_counts().reset_index()
    ip_counts.columns = ['IP Address', 'Count']

    # Creating a horizontal bar chart using Plotly Express
    fig_top_ips = px.bar(ip_counts.head(10), x='Count', y='IP Address', orientation='h',
                 title='Top 10 Most Active IP Addresses')

    ####################################################################################################################
    # Streamlit app
    st.set_page_config(layout="wide")

    st.title('NGINX log visualizer')

    # Create a 3-column layout
    col1, col2, col3 = st.columns(3, gap="medium")
    col4, col5 = st.columns([2, 1])

    with st.container():
        # Column 1
        with col1:
            st.plotly_chart(fig_traffic_over_time, use_container_width=True)
            st.plotly_chart(fig_most_requested_endpoints, use_container_width=True)

        # Column 2
        with col2:
            st.plotly_chart(fig_hourly_requests, use_container_width=True)
            st.plotly_chart(fig_top_http_responses, use_container_width=True)

        # Column 3
        with col3:
            st.plotly_chart(fig_top_ips, use_container_width=True)
            st.plotly_chart(fig_ip_locations, use_container_width=True)

    with st.container():
        with col4:
            st.write(df, use_container_width=True)

        with col5:
            st.write(user_agents_count, use_container_width=True)

    with st.container():
        st.write('Author: Michael Duschek, 2023')


# Check if the script is being run as the main program
if __name__ == '__main__':
    env_values = dotenv_values('.env')
    ipgeolocation_key = env_values.get('IP_GEOLOCATION_KEY')
    # print(get_ip_geolocation('1.1.1.1'))    # for testing purposes (API KEY)

    main()
