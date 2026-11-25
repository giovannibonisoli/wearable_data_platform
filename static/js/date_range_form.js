/*
 * From/To date range form shared by the device detail charts.
 *
 * Expected markup: a GET form with inputs named "start", "end" and "adjusted"
 * (hidden), and data attributes data-today="YYYY-MM-DD" and data-max-months.
 *
 * On submit, keeps start <= end <= today and the range within data-max-months
 * calendar months, correcting the date the user edited last (the other one is
 * kept). "adjusted" is set to "1" when a correction was made, so the page can
 * say so after reloading. The server applies the same rules as a fallback.
 */
(function () {
    // ISO dates (YYYY-MM-DD) compare correctly as strings.
    function shiftMonths(iso, months) {
        const [y, m, d] = iso.split('-').map(Number);
        const index = y * 12 + (m - 1) + months;
        const year = Math.floor(index / 12);
        const month = index % 12;                                   // 0-based
        const lastDay = new Date(Date.UTC(year, month + 1, 0)).getUTCDate();
        const date = new Date(Date.UTC(year, month, Math.min(d, lastDay)));
        return date.toISOString().slice(0, 10);
    }

    function initDateRangeForm(form) {
        if (!form) return;
        const startInput = form.elements.start;
        const endInput = form.elements.end;
        const today = form.dataset.today;
        const maxMonths = parseInt(form.dataset.maxMonths, 10);
        const initial = { start: startInput.value, end: endInput.value };
        let lastEdited = 'start';

        startInput.addEventListener('input', function () { lastEdited = 'start'; });
        endInput.addEventListener('input', function () { lastEdited = 'end'; });

        form.addEventListener('submit', function () {
            // An empty or incomplete date falls back to the current value.
            let start = startInput.value || initial.start;
            let end = endInput.value || initial.end;
            let adjusted = false;

            // The other date only has to be valid on its own (not in the future).
            if (lastEdited === 'start' && end > today) { end = today; adjusted = true; }
            if (lastEdited === 'end' && start > today) { start = today; adjusted = true; }

            if (lastEdited === 'start') {
                let fixed = start > end ? end : start;
                const earliest = shiftMonths(end, -maxMonths);
                if (fixed < earliest) fixed = earliest;
                if (fixed !== start) { start = fixed; adjusted = true; }
            } else {
                let fixed = end > today ? today : end;
                if (fixed < start) fixed = start;
                let latest = shiftMonths(start, maxMonths);
                if (latest > today) latest = today;
                if (fixed > latest) fixed = latest;
                if (fixed !== end) { end = fixed; adjusted = true; }
            }

            startInput.value = start;
            endInput.value = end;
            form.elements.adjusted.value = adjusted ? '1' : '';
        });
    }

    document.querySelectorAll('form[data-date-range]').forEach(initDateRangeForm);
})();
