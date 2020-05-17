"""Load config variables for pippeline."""

CONFIG_VARS = {

    'TMP_DATA_DIR': 'tmp_data',
    'OP_DATA_DIR': '../data',

    'DATE_COLUMNS': [
        'Declaration Date',
        'date_call_made',
        'as_at_date',
        'date'],

    'FLOAT_COLUMNS': [],

    'INT_COLUMNS': [
        'target_net_zero_year',
        'unique_visits',
        'follows',
        'likes',
        'views',
        'total_sales'],

    'GOOGLE_API_KEY_FILE': 'google_auth_prod_creds.json',

    'GOOGLE_API_SCOPE': [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/analytics.readonly'],

    'GOOGLE_ANALYTICS_VIEWS': [
        {'domain': 'https://rebellion.earth',
         'view_id': '184845394'},
    ],
'GLOBAL_INSTAGRAM_ID': '17841408183412514',
'GLOBAL_FACEBOOK_ID': '239675493315233',
'UK_INSTAGRAM_ID': '',
'UK_FACEBOOK_ID': '401847960644025',
    'STM': {
        'Local Authorities': {
            'Local Authority Code (LAD18NM)': 'code',
            'ONS Local Authority  Name (LAD18NM)': 'ons_la_name',
            'XR LA Name': 'xr_la_name',
            'Declared?': 'is_declared',
            'Declaration Date': 'declaration_date',
            'Target Net Zero Year':  'target_net_zero_year',
            'Source': 'source'},
        'Political Parties': {
            'Organisation Name': 'org_name',
            'Is Political Org?': 'is_political_org',
            'Date Call Made': 'date_call_made',
            'Target Net Zero Year':  'target_net_zero_year',
            'Earliest Year': 'earliest_year',
            'Latest Year': 'latest_year',
            'Percentage of Vote in Last Election': 'vote_pcnt'},
        'Social Media': {
            'Platform': 'platform',
            'Account ID': 'account_id',
            'Date': 'date',
            'Daily Follows': 'follows',
            'Daily Likes': 'likes',
            'Daily Views': 'views',
            'Cumulative Follows': 'follows_cum',
            'Cumulative Likes': 'likes_cum',
            'Cumulative Views': 'views_cum'},
        'Website': {
            'domain': 'domain',
            'ga:date': 'date',
            'ga:pageviews': 'page_views',
            'ga:sessions': 'sessions'},
        'Book Sales': {
            'Date': 'date',
            'UK Sales': 'sales'},
        'Instagram': {
            'Platform': 'platform',
            'Account ID': 'account_id',
            'Date': 'date',
            'New Daily Followers': 'follows',
            'Daily Likes': 'likes'},
    }
}
