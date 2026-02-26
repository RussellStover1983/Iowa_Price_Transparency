/* Iowa Price Transparency — Frontend */

const API = window.location.origin;

// --- State ---
const state = {
    selectedCodes: [],     // [{code, description, category}]
    payers: [],
    cities: new Set(),
    counties: new Set(),
};

// --- DOM refs ---
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

const searchInput = $("#search-input");
const searchResults = $("#search-results");
const selectedCodesEl = $("#selected-codes");
const codeChipsEl = $("#code-chips");
const selectedCountEl = $("#selected-count");
const compareBtn = $("#compare-btn");
const compareResultsEl = $("#compare-results");
const statsSummaryEl = $("#stats-summary");
const procedureCardsEl = $("#procedure-cards");
const filterPayer = $("#filter-payer");
const filterCity = $("#filter-city");
const filterSort = $("#filter-sort");
const providerListEl = $("#provider-list");
const providerCityFilter = $("#provider-city-filter");
const providerCountyFilter = $("#provider-county-filter");

// --- Navigation ---
$$(".nav-link").forEach((link) => {
    link.addEventListener("click", (e) => {
        e.preventDefault();
        const page = link.dataset.page;
        $$(".nav-link").forEach((l) => l.classList.remove("active"));
        link.classList.add("active");
        $$(".page").forEach((p) => p.classList.remove("active"));
        $(`#page-${page}`).classList.add("active");

        if (page === "providers") loadProviders();
    });
});

// --- Search ---
let searchTimeout = null;

searchInput.addEventListener("input", () => {
    clearTimeout(searchTimeout);
    const q = searchInput.value.trim();
    if (q.length < 2) {
        searchResults.classList.add("hidden");
        return;
    }
    searchTimeout = setTimeout(() => searchCPT(q), 250);
});

searchInput.addEventListener("focus", () => {
    if (searchResults.children.length > 0 && searchInput.value.trim().length >= 2) {
        searchResults.classList.remove("hidden");
    }
});

document.addEventListener("click", (e) => {
    if (!e.target.closest(".search-box")) {
        searchResults.classList.add("hidden");
    }
});

async function searchCPT(query) {
    try {
        const res = await fetch(`${API}/v1/cpt/search?q=${encodeURIComponent(query)}&limit=15`);
        if (!res.ok) return;
        const data = await res.json();
        renderSearchResults(data.results);
    } catch (err) {
        console.error("Search error:", err);
    }
}

function renderSearchResults(results) {
    if (!results.length) {
        searchResults.innerHTML = '<div class="search-item"><span class="desc">No procedures found</span></div>';
        searchResults.classList.remove("hidden");
        return;
    }

    const alreadySelected = new Set(state.selectedCodes.map((c) => c.code));

    searchResults.innerHTML = results
        .map((r) => {
            const disabled = alreadySelected.has(r.code);
            const names = r.common_names?.length ? r.common_names.slice(0, 3).join(", ") : "";
            return `
                <div class="search-item ${disabled ? "disabled" : ""}"
                     data-code="${r.code}"
                     data-desc="${escapeAttr(r.description)}"
                     data-category="${escapeAttr(r.category || "")}">
                    <div>
                        <span class="code">CPT ${r.code}</span>
                        <span class="desc">${escapeHtml(r.description)}</span>
                    </div>
                    ${r.category ? `<div class="category">${escapeHtml(r.category)}</div>` : ""}
                    ${names ? `<div class="names">${escapeHtml(names)}</div>` : ""}
                    ${disabled ? '<div class="names">Already selected</div>' : ""}
                </div>
            `;
        })
        .join("");

    searchResults.classList.remove("hidden");

    searchResults.querySelectorAll(".search-item:not(.disabled)").forEach((item) => {
        item.addEventListener("click", () => {
            addCode(item.dataset.code, item.dataset.desc, item.dataset.category);
            searchResults.classList.add("hidden");
            searchInput.value = "";
        });
    });
}

// --- Selected Codes ---
function addCode(code, description, category) {
    if (state.selectedCodes.length >= 10) return;
    if (state.selectedCodes.find((c) => c.code === code)) return;
    state.selectedCodes.push({ code, description, category });
    renderChips();
}

function removeCode(code) {
    state.selectedCodes = state.selectedCodes.filter((c) => c.code !== code);
    renderChips();
    if (state.selectedCodes.length === 0) {
        compareResultsEl.classList.add("hidden");
    }
}

function renderChips() {
    if (state.selectedCodes.length === 0) {
        selectedCodesEl.classList.add("hidden");
        return;
    }

    selectedCodesEl.classList.remove("hidden");
    selectedCountEl.textContent = `(${state.selectedCodes.length}/10)`;

    codeChipsEl.innerHTML = state.selectedCodes
        .map(
            (c) => `
        <span class="chip">
            <strong>${c.code}</strong> ${escapeHtml(truncate(c.description, 40))}
            <span class="remove" data-code="${c.code}">&times;</span>
        </span>
    `
        )
        .join("");

    codeChipsEl.querySelectorAll(".remove").forEach((btn) => {
        btn.addEventListener("click", () => removeCode(btn.dataset.code));
    });
}

// --- Compare ---
compareBtn.addEventListener("click", () => runCompare());

async function runCompare() {
    if (!state.selectedCodes.length) return;
    compareBtn.disabled = true;
    compareBtn.textContent = "Loading...";

    const codes = state.selectedCodes.map((c) => c.code).join(",");
    const params = new URLSearchParams({ codes });
    if (filterPayer.value) params.set("payer", filterPayer.value);
    if (filterCity.value) params.set("city", filterCity.value);
    if (filterSort.value) params.set("sort", filterSort.value);

    try {
        const res = await fetch(`${API}/v1/compare?${params}`);
        if (!res.ok) {
            const err = await res.json();
            alert(err.detail || "Error comparing prices");
            return;
        }
        const data = await res.json();
        renderCompareResults(data);
    } catch (err) {
        console.error("Compare error:", err);
        alert("Failed to fetch comparison data");
    } finally {
        compareBtn.disabled = false;
        compareBtn.textContent = "Compare Prices";
    }
}

function renderCompareResults(data) {
    compareResultsEl.classList.remove("hidden");

    // Render stats
    if (data.stats && data.stats.length) {
        statsSummaryEl.innerHTML = `
            <h3 style="margin-bottom: 12px; font-size: 1.1rem;">Price Summary</h3>
            <div class="stats-grid">
                ${data.stats.map(renderStatCard).join("")}
            </div>
        `;
    } else {
        statsSummaryEl.innerHTML = "";
    }

    // Render procedure cards
    if (!data.procedures.length) {
        procedureCardsEl.innerHTML = '<div class="empty-state">No pricing data found for the selected filters.</div>';
        return;
    }

    procedureCardsEl.innerHTML = data.procedures.map(renderProcedureCard).join("");
}

function renderStatCard(stat) {
    const savingsClass = stat.potential_savings > 10000 ? "high" : stat.potential_savings > 3000 ? "medium" : "low";
    return `
        <div class="stat-card">
            <h4>${escapeHtml(stat.description || stat.billing_code)}</h4>
            <div class="stat-code">CPT ${stat.billing_code} &mdash; ${stat.provider_count} providers, ${stat.rate_count} rates</div>
            <div class="stat-row"><span class="label">Lowest</span><span class="value">${formatPrice(stat.min_rate)}</span></div>
            <div class="stat-row"><span class="label">Highest</span><span class="value">${formatPrice(stat.max_rate)}</span></div>
            <div class="stat-row"><span class="label">Median</span><span class="value">${formatPrice(stat.median_rate)}</span></div>
            <div class="stat-row"><span class="label">Average</span><span class="value">${formatPrice(stat.avg_rate)}</span></div>
            <div class="savings ${savingsClass}">Potential savings: ${formatPrice(stat.potential_savings)}</div>
        </div>
    `;
}

function renderProcedureCard(proc) {
    if (!proc.providers.length) {
        return `
            <div class="procedure-card">
                <div class="procedure-header">
                    <h3>CPT ${proc.billing_code} &mdash; ${escapeHtml(proc.description || "Unknown")}</h3>
                    <div class="meta">${escapeHtml(proc.category || "")}</div>
                </div>
                <div class="empty-state">No providers found</div>
            </div>
        `;
    }

    // Find global min/max across all providers for this procedure
    const allMin = Math.min(...proc.providers.map((p) => p.min_rate));
    const allMax = Math.max(...proc.providers.map((p) => p.max_rate));

    const rows = proc.providers
        .map((provider) => {
            const isLowest = provider.min_rate === allMin;
            const isHighest = provider.max_rate === allMax;
            const rateClass = isLowest ? "rate-lowest" : isHighest ? "rate-highest" : "";

            // Show range or single price
            const priceDisplay =
                provider.min_rate === provider.max_rate
                    ? formatPrice(provider.min_rate)
                    : `${formatPrice(provider.min_rate)} &ndash; ${formatPrice(provider.max_rate)}`;

            const badge = isLowest
                ? '<span class="rate-badge lowest">Lowest</span>'
                : isHighest && proc.providers.length > 1
                  ? '<span class="rate-badge highest">Highest</span>'
                  : "";

            const payers = provider.rates
                .map((r) => r.payer_name)
                .filter((v, i, a) => a.indexOf(v) === i)
                .join(", ");

            return `
                <tr>
                    <td>
                        <div class="provider-name">${escapeHtml(provider.provider_name)}</div>
                        <div class="provider-location">${escapeHtml(provider.city || "")}, ${escapeHtml(provider.county || "")} County</div>
                    </td>
                    <td><span class="rate-amount ${rateClass}">${priceDisplay}</span> ${badge}</td>
                    <td class="rate-payer">${escapeHtml(payers)}</td>
                </tr>
            `;
        })
        .join("");

    return `
        <div class="procedure-card">
            <div class="procedure-header">
                <h3>CPT ${proc.billing_code} &mdash; ${escapeHtml(proc.description || "Unknown")}</h3>
                <div class="meta">${escapeHtml(proc.category || "")} &middot; ${proc.provider_count} providers</div>
            </div>
            <div class="table-wrapper">
                <table class="provider-table">
                    <thead>
                        <tr>
                            <th>Provider</th>
                            <th>Negotiated Rate</th>
                            <th>Payers</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        </div>
    `;
}

// --- Providers Page ---
async function loadProviders() {
    providerListEl.innerHTML = '<div class="loading"><div class="spinner"></div>Loading providers...</div>';

    const params = new URLSearchParams();
    if (providerCityFilter.value) params.set("city", providerCityFilter.value);
    if (providerCountyFilter.value) params.set("county", providerCountyFilter.value);

    try {
        const res = await fetch(`${API}/v1/providers?${params}`);
        if (!res.ok) return;
        const data = await res.json();
        renderProviders(data);
    } catch (err) {
        providerListEl.innerHTML = '<div class="empty-state">Failed to load providers</div>';
    }
}

function renderProviders(data) {
    if (!data.providers.length) {
        providerListEl.innerHTML = '<div class="empty-state">No providers found</div>';
        return;
    }

    // Populate city/county filters from the full unfiltered data (only on first load)
    if (state.cities.size === 0) {
        data.providers.forEach((p) => {
            if (p.city) state.cities.add(p.city);
            if (p.county) state.counties.add(p.county);
        });
        populateProviderFilters();
    }

    providerListEl.innerHTML = `
        <div class="provider-grid">
            ${data.providers.map(renderProviderCard).join("")}
        </div>
    `;
}

function renderProviderCard(provider) {
    return `
        <div class="provider-card">
            <h3>${escapeHtml(provider.name)}</h3>
            <div class="detail">${escapeHtml(provider.city || "")}, Iowa ${escapeHtml(provider.zip_code || "")}</div>
            <div class="detail">${escapeHtml(provider.county || "")} County</div>
            <div class="detail" style="text-transform: capitalize;">${escapeHtml(provider.facility_type || "")}</div>
            <div class="counts">
                <span class="count-item"><strong>${provider.procedure_count}</strong> procedures</span>
                <span class="count-item"><strong>${provider.payer_count}</strong> payers</span>
            </div>
        </div>
    `;
}

function populateProviderFilters() {
    const sortedCities = [...state.cities].sort();
    const sortedCounties = [...state.counties].sort();

    sortedCities.forEach((c) => {
        providerCityFilter.add(new Option(c, c));
        // Also populate compare city filter
        filterCity.add(new Option(c, c));
    });

    sortedCounties.forEach((c) => {
        providerCountyFilter.add(new Option(c + " County", c));
    });
}

providerCityFilter.addEventListener("change", loadProviders);
providerCountyFilter.addEventListener("change", loadProviders);

// --- Load payers for filter ---
async function loadPayers() {
    try {
        const res = await fetch(`${API}/v1/payers`);
        if (!res.ok) return;
        state.payers = await res.json();
        state.payers.forEach((p) => {
            filterPayer.add(new Option(p.name, p.short_name));
        });
    } catch (err) {
        console.error("Failed to load payers:", err);
    }
}

// --- Utilities ---
function formatPrice(amount) {
    return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" }).format(amount);
}

function escapeHtml(str) {
    if (!str) return "";
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
}

function escapeAttr(str) {
    if (!str) return "";
    return str.replace(/"/g, "&quot;").replace(/'/g, "&#39;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function truncate(str, len) {
    if (!str) return "";
    return str.length > len ? str.slice(0, len) + "..." : str;
}

// --- Init ---
loadPayers();

// Also preload cities/counties for compare filter
(async () => {
    try {
        const res = await fetch(`${API}/v1/providers`);
        if (!res.ok) return;
        const data = await res.json();
        data.providers.forEach((p) => {
            if (p.city) state.cities.add(p.city);
            if (p.county) state.counties.add(p.county);
        });
        populateProviderFilters();
    } catch (err) {
        // Filters just won't be populated — non-critical
    }
})();
