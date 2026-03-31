document.addEventListener('DOMContentLoaded', function () {
  const currentPath = window.location.pathname;
  document.querySelectorAll('[data-nav-link]').forEach((link) => {
    const href = link.getAttribute('href');
    if (href && (currentPath === href || (href !== '/' && currentPath.startsWith(href)))) {
      link.classList.add('active');
    }
  });

  document.querySelectorAll('[data-copy-target]').forEach((button) => {
    button.addEventListener('click', async function () {
      const target = document.querySelector(button.getAttribute('data-copy-target'));
      if (!target) return;
      try {
        await navigator.clipboard.writeText((target.innerText || target.textContent || '').trim());
        const original = button.dataset.originalLabel || button.textContent;
        button.dataset.originalLabel = original;
        button.textContent = 'คัดลอกแล้ว';
        setTimeout(() => { button.textContent = original; }, 1200);
      } catch (_err) {
        window.alert('คัดลอกไม่สำเร็จ');
      }
    });
  });

  document.querySelectorAll('[data-chart-source]').forEach((el) => {
    const sourceId = el.getAttribute('data-chart-source');
    const script = document.getElementById(sourceId);
    if (!script || !window.Highcharts) return;
    try {
      const config = JSON.parse(script.textContent || '{}');
      if (!config.chart) config.chart = {};
      if (!config.chart.renderTo) config.chart.renderTo = el;
      window.Highcharts.chart(el, config);
    } catch (err) {
      console.error('chart init failed', sourceId, err);
    }
  });
});
