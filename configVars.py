"""Load config variables for pippeline."""

from datetime import datetime

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
         'view_id': '220233948',
         'old_view_id': '184845394'},
    ],

    'GLOBAL_INSTAGRAM_ID': '17841408183412514',
    'GLOBAL_INSTAGRAM_ACCOUNT_CREATION_DATE': datetime.strptime('2018-07-18', "%Y-%m-%d"),

    'GLOBAL_FACEBOOK_ID': '239675493315233',
    'GLOBAL_FACEBOOK_ACCOUNT_CREATION_DATE': datetime.strptime('2018-07-27', "%Y-%m-%d"),

    'FACEBOOK_API_URL_LIKES': "https://graph.facebook.com/v5.0/{api_id}/media?access_token={api_key}&fields=like_count%2Ctimestamp%2Cpermalink",
    'FACEBOOK_API_URL_FOLLOWS': "https://graph.facebook.com/v5.0/{api_id}/insights?pretty=0&since={since}&until={until}&metric={metric}&period=day&access_token={api_key}",
    'FACEBOOK_API_URL_FB_POSTS': "https://graph.facebook.com/v5.0/{api_id}/posts?access_token={api_key}",
    'FACEBOOK_API_URL_FB_LIKES': "https://graph.facebook.com/{post_id}?fields=likes.summary(true)&access_token={api_key}",

    'FACEBOOK_API_FB_DAY_RANGE': 93,
    'FACEBOOK_API_IG_DAY_RANGE': 30,

    'ACTION_NETWORK_NEXTCLOUD_FILE_URL': "https://cloud.extinctionrebellion.uk/remote.php/dav/files/brianspurling/AutoUpload/cumulative_subscribers.csv",

    'STM': {
        'Local Authorities': {
            'Local Authority Code (LAD18NM)': 'code',
            'ONS Local Authority  Name (LAD18NM)': 'ons_la_name',
            'XR LA Name': 'xr_la_name',
            'Declared?': 'is_declared',
            'Declaration Date': 'declaration_date',
            'Target Net Zero Year':  'target_net_zero_year',
            'Source': 'source',
            'Source of plan or progress report': 'action_plan'},
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
        'Website - Old': {
            'domain': 'domain',
            'ga:date': 'date',
            'ga:pageviews': 'page_views',
            'ga:sessions': 'sessions'},
        'Website': {
            'domain': 'domain',
            'ga:date': 'date',
            'ga:pageviews': 'page_views',
            'ga:sessions': 'sessions'},
        'Action Network': {
            'date': 'date',
            'dailysubscribes': 'daily',
            'cumulativesubscribes': 'cumulative'},
        'Book Sales': {
            'Date': 'date',
            'UK Sales': 'sales'},
        'Instagram': {
            'Date': 'date',
            'Daily Likes': 'likes',
            'Cumulative Follows': 'follows_cum'},
        'Facebook': {
            'Date': 'date',
            'Daily Likes': 'likes',
            'Cumulative Follows': 'follows_cum'},
    }
}
