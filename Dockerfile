FROM apache/airflow:3.0.1

USER root

# Install Chrome and ChromeDriver
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl

# Install Chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update && apt-get install -y google-chrome-stable

# Install ChromeDriver
RUN CHROME_VERSION=$(google-chrome --version | awk '{print $3}' | cut -d '.' -f 1) && \
    CHROMEDRIVER_URL=$(curl -s "https://googlechromelabs.github.io/chrome-for-testing/latest-versions-per-milestone-with-downloads.json" | \
    python3 -c "import sys, json; v='$CHROME_VERSION'; data=json.load(sys.stdin); print(next((d['url'] for d in data['milestones'][v]['downloads']['chromedriver'] if d['platform'] == 'linux64'), ''))") && \
    wget -q "$CHROMEDRIVER_URL" -O chromedriver.zip && \
    unzip chromedriver.zip && \
    mv chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver && \
    rm -rf chromedriver.zip chromedriver-linux64


# Switch back to airflow user
USER airflow

# Install Python packages
RUN pip install --no-cache-dir selenium webdriver-manager transformers torch numpy sentencepiece

RUN chromedriver --version



# Install dbt
#RUN pip install dbt-core dbt-postgres



