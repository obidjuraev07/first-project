import pandas as pd
import numpy as np
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from django.conf import settings
from clickhouse_driver import Client as ClickHouseClient
from .models import Report


def get_client(**config):
    """Get ClickHouse client"""
    return ClickHouseClient(**config)


class ClickHouseCumulativeReachView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, id):
        # Get the report and filters
        try:
            report = Report.objects.get(id=id, user=request.user)
        except Report.DoesNotExist:
            return Response({'error': 'Report not found'}, status=404)
        filters = report.filters

        # Extract filters
        region_ids = filters.get('region')
        gender_ids = filters.get('gender')
        age_group_ids = filters.get('age')
        start_date = filters.get('start_date')
        end_date = filters.get('end_date')
        top_n = int(request.query_params.get('top_n', 4))
        period = request.query_params.get('period', 'month')
        selected_platforms = request.query_params.getlist('platforms', [])

        # Ensure all filters are lists for IN clause
        def ensure_list(val):
            if isinstance(val, list):
                return val
            if val is None:
                return []
            return [val]

        region_ids = ensure_list(region_ids)
        gender_ids = ensure_list(gender_ids)
        age_group_ids = ensure_list(age_group_ids)

        # Platform filter SQL
        platform_filter = ''
        if selected_platforms:
            platform_list = ','.join([f"'{p}'" for p in selected_platforms])
            platform_filter = f"AND td.source_name IN ({platform_list})"

        # Build the query to get individual platform reach percentages
        query = f'''
        SELECT
            td.source_name AS platform,
            COUNT(DISTINCT td.msisdn) AS user_count,
            ROUND(COUNT(DISTINCT td.msisdn) * 100.0 / population.total_pop, 2) AS reach_percent
        FROM traffic_daily td
        CROSS JOIN (
            SELECT COUNT(DISTINCT msisdn) AS total_pop
            FROM traffic_daily
            WHERE region_id IN ({','.join(map(str, region_ids))})
              AND gender_id IN ({','.join(map(str, gender_ids))})
              AND age_group_id IN ({','.join(map(str, age_group_ids))})
              AND pdate BETWEEN '{start_date}' AND '{end_date}'
        ) population
        WHERE td.region_id IN ({','.join(map(str, region_ids))})
          AND td.gender_id IN ({','.join(map(str, gender_ids))})
          AND td.age_group_id IN ({','.join(map(str, age_group_ids))})
          AND td.pdate BETWEEN '{start_date}' AND '{end_date}'
          {platform_filter}
        GROUP BY td.source_name, population.total_pop
        ORDER BY user_count DESC
        LIMIT {top_n}
        '''

        client = get_client(**settings.CLICKHOUSE_CONFIG)
        result = client.query(query)

        # Convert result to pandas DataFrame for easier manipulation
        df = pd.DataFrame(result.result_rows, columns=['platform', 'user_count', 'reach_percent'])
        
        if df.empty:
            return Response([])

        # Convert reach percentages to fractions (divide by 100)
        reach_fractions = df['reach_percent'] / 100.0
        
        # Calculate probability that a person was NOT reached by any platform
        # Using the formula: P(not reached) = (1 - r1) * (1 - r2) * (1 - r3) * ...
        prob_not_reached = np.prod(1 - reach_fractions)
        
        # Calculate cumulative reach: 1 - P(not reached)
        cumulative_reach_percent = (1 - prob_not_reached) * 100.0
        
        # Calculate cumulative user count based on total population
        total_pop = df['user_count'].iloc[0] / (df['reach_percent'].iloc[0] / 100.0) if not df.empty else 0
        cumulative_user_count = int(cumulative_reach_percent * total_pop / 100.0)

        # Format response for frontend
        response = []
        for idx, row in df.iterrows():
            platform = row['platform']
            user_count = row['user_count']
            reach_percent = row['reach_percent']
            
            # For label: e.g. "20% - 5M"
            if user_count >= 1_000_000:
                label = f"{reach_percent}% - {round(user_count / 1_000_000, 1)}M"
            elif user_count >= 1_000:
                label = f"{reach_percent}% - {round(user_count / 1_000, 1)}K"
            else:
                label = f"{reach_percent}% - {user_count}"
                
            response.append({
                "platform": platform,
                "percent": reach_percent,
                "count": user_count
            })
        
        # Add cumulative reach information
        response.append({
            "platform": "Cumulative",
            "percent": round(cumulative_reach_percent, 2),
            "count": cumulative_user_count
        })
        
        return Response(response)