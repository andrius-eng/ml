import { useEffect, useRef } from 'react';
import { Chart } from 'chart.js';

function barColor(zArr) {
  return zArr.map((z) => (z >= 0 ? 'rgba(239,68,68,0.7)' : 'rgba(59,130,246,0.7)'));
}

function baseScaleOpts(label) {
  return {
    x: {
      grid: { color: 'rgba(255,255,255,0.06)' },
      ticks: { color: 'rgba(255,255,255,0.6)' },
    },
    y: {
      grid: { color: 'rgba(255,255,255,0.08)' },
      ticks: { color: 'rgba(255,255,255,0.6)' },
      title: { display: true, text: label, color: 'rgba(255,255,255,0.45)' },
    },
  };
}

function CityBarChart({ canvasRef, title, labels, data: chartData }) {
  const chartRef = useRef(null);

  useEffect(() => {
    if (!canvasRef.current || !labels || !chartData) return;
    if (chartRef.current) chartRef.current.destroy();

    chartRef.current = new Chart(canvasRef.current, {
      type: 'bar',
      data: {
        labels,
        datasets: [
          {
            label: title,
            data: chartData,
            backgroundColor: barColor(chartData),
            borderRadius: 4,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          title: { display: true, text: title, color: '#f7f7f7' },
        },
        scales: baseScaleOpts('z-score vs 1991\u20132020'),
      },
    });

    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
  }, [canvasRef, labels, chartData, title]);

  return null;
}

export function CityComparisonCharts({ data }) {
  const tempCanvasRef = useRef(null);
  const precipCanvasRef = useRef(null);

  const w = data.lithuania_weather;
  const cities = w.city_rankings.temperature.map((r) => r.city);
  const tempZ = w.city_rankings.temperature.map((r) => r.z_score);
  const precipZOrdered = cities.map((city) => {
    const match = w.city_rankings.precipitation.find((p) => p.city === city);
    return match ? match.z_score : 0;
  });

  return (
    <section className="card" id="ytd-section">
      <div className="section-header">
        <div>
          <h2>Lithuania YTD &mdash; City Anomaly Comparison</h2>
          <p className="section-desc">
            How far each city&rsquo;s temperature and precipitation deviate from the
            long-term normal this year, expressed as z-scores vs. the 1991&ndash;2025
            climatology, period{' '}
            <span>{w.period}</span>.
            A z-score of 0 is perfectly average; &plusmn;1 covers ~68% of historical years;
            beyond &plusmn;2 is statistically rare.
            Use this chart to quickly spot which cities are driving a national
            anomaly signal &mdash; or diverging from the rest of the country.
          </p>
        </div>
      </div>
      <div className="chart-row">
        <div className="chart-wrap chart-wrap--half">
          <canvas ref={tempCanvasRef} />
          <CityBarChart
            canvasRef={tempCanvasRef}
            title="YTD Temperature z-score"
            labels={cities}
            data={tempZ}
          />
        </div>
        <div className="chart-wrap chart-wrap--half">
          <canvas ref={precipCanvasRef} />
          <CityBarChart
            canvasRef={precipCanvasRef}
            title="YTD Precipitation z-score"
            labels={cities}
            data={precipZOrdered}
          />
        </div>
      </div>
    </section>
  );
}
