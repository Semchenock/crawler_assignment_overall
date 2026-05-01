import time
import json
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime
from collections import defaultdict
from typing import Optional
from urllib.parse import urlparse


class CrawlerStats:
    def __init__(self):
        self.successful_requests_count = 0
        self.failed_requests_count = 0
        self.processed_urls_by_domain = defaultdict(int)
        self.status_codes = defaultdict(int)
        self.started_at: Optional[float] = None
        self.ended_at: Optional[float] = None

    def reset(self):
        self.successful_requests_count = 0
        self.failed_requests_count = 0
        self.processed_urls_by_domain = defaultdict(int)
        self.status_codes = defaultdict(int)
        self.started_at: Optional[float] = None
        self.ended_at: Optional[float] = None

    def start_crawling(self):
        self.reset()
        self.started_at = time.monotonic()

    def end_crawling(self):
        self.ended_at = time.monotonic()

    def handle_successful_request(self, status_code: int):
        self.successful_requests_count += 1
        self.status_codes[status_code] += 1

    def handle_failed_request(self, status_code: int):
        self.failed_requests_count += 1
        self.status_codes[status_code] += 1

    def handle_processed_url(self, url: str):
        domain = urlparse(url).hostname
        self.processed_urls_by_domain[domain] += 1

    def get_stats(self):
        top_domains = sorted(
            self.processed_urls_by_domain.items(),
            key=lambda x: x[1],
            reverse=True
        )

        top_error_codes = sorted(
            self.status_codes.items(),
            key=lambda x: x[1],
            reverse=True
        )

        total_processed_urls = sum(self.processed_urls_by_domain.values())

        end_time = time.monotonic() if self.ended_at is None else self.ended_at

        total_time = end_time - self.started_at

        return {
            'total_processed_urls': total_processed_urls,
            'successful_requests_count': self.successful_requests_count,
            'failed_requests_count': self.failed_requests_count,
            'average_speed': total_time / total_processed_urls if total_processed_urls !=0 else 0,
            'top_domains': top_domains,
            'top_error_codes': top_error_codes,
            'total_time': total_time,
        }

    def export_to_json(self, filename: str = 'crawler_stats.json'):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(
                self.get_stats(),
                f,
                ensure_ascii=False,
                indent=2,
                default=str
            )

    def export_to_html_report(self, filename: str = 'crawler_report.html'):
        data = self.get_stats()

        df_domains = pd.DataFrame(data['top_domains'], columns=["domain", "count"])
        df_errors = pd.DataFrame(data['top_error_codes'], columns=["error_code", "count"])

        domains_chart = px.bar(
            df_domains,
            x="domain",
            y="count",
            title="Top Domains"
        )

        errors_chart = px.bar(
            df_errors,
            x="error_code",
            y="count",
            title="Top Error Codes"
        )

        summary_chart = go.Figure(data=[
            go.Bar(name="Success", x=["Requests"], y=[data['successful_requests_count']]),
            go.Bar(name="Failed", x=["Requests"], y=[data['failed_requests_count']]),
        ])
        summary_chart.update_layout(barmode='group', title="Success vs Failed")

        domains_html = domains_chart.to_html(full_html=False, include_plotlyjs='cdn')
        errors_html = errors_chart.to_html(full_html=False, include_plotlyjs=False)
        summary_html = summary_chart.to_html(full_html=False, include_plotlyjs=False)

        table_domains = df_domains.to_html(index=False)
        table_errors = df_errors.to_html(index=False)

        html = f"""
        <html>
        <head>
            <meta charset="utf-8">
            <title>Crawler Report</title>
            <style>
                body {{
                    font-family: Arial;
                    margin: 40px;
                    background: #f7f7f7;
                }}
                h1, h2 {{
                    color: #333;
                }}
                .card {{
                    background: white;
                    padding: 20px;
                    margin-bottom: 20px;
                    border-radius: 10px;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                }}
                table {{
                    border-collapse: collapse;
                    width: 100%;
                }}
                th, td {{
                    border: 1px solid #ddd;
                    padding: 8px;
                }}
                th {{
                    background-color: #eee;
                }}
            </style>
        </head>
        <body>

        <h1>Crawler Report</h1>
        <p>Generated at: {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")} UTC</p>

        <div class="card">
            <h2>Summary</h2>
            <ul>
                <li>Total processed URLs: {data['total_processed_urls']}</li>
                <li>Successful requests: {data['successful_requests_count']}</li>
                <li>Failed requests: {data['failed_requests_count']}</li>
                <li>Average latency (sec): {data['average_speed']:.4f}</li>
                <li>Total time (sec): {data['total_time']:.2f}</li>
            </ul>
            {summary_html}
        </div>

        <div class="card">
            <h2>Top Domains</h2>
            {domains_html}
            {table_domains}
        </div>

        <div class="card">
            <h2>Top Error Codes</h2>
            {errors_html}
            {table_errors}
        </div>

        </body>
        </html>
        """

        with open(filename, "w", encoding="utf-8") as f:
            f.write(html)


