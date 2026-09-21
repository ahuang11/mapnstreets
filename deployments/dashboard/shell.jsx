// MapnStreets shell — search-first layout with pn.Tabs for content.
//
// Two modes: a landing page (before any search) and a results view.
// Tabs are handled by Panel's own Tabs widget (passed as `content`
// child), which manages ECharts/Tabulator lifecycle correctly.

const EXAMPLES = ["Main St", "Roosevelt*", "River Rd"];

function LedgerCell({ label, value, caption }) {
  return (
    <div className="mns-ledger-cell">
      <div className="mns-ledger-label">{label}</div>
      <div className="mns-ledger-value">{value}</div>
      <div className="mns-ledger-caption">{caption}</div>
    </div>
  );
}

export function render({ model }) {
  const [street, setStreet] = model.useState("street");
  const [searchNonce, setSearchNonce] = model.useState("search_nonce");
  const [kpis] = model.useState("kpis");
  const [countMessage] = model.useState("count_message");
  const [error] = model.useState("error");

  const [teaser] = model.useState("teaser");

  const [draft, setDraft] = React.useState(street);
  React.useEffect(() => setDraft(street), [street]);

  const hasSearched = searchNonce > 0;

  function search(value) {
    const next = (value ?? draft).trim();
    if (!next) {
      setDraft(street);
      return;
    }
    setStreet(next);
    setSearchNonce(searchNonce + 1);
  }

  function commitIfChanged() {
    const next = draft.trim();
    if (next && next !== street) search(next);
    else setDraft(street);
  }

  if (error) {
    return (
      <div className="mns-page">
        <header className="mns-header">
          <span className="mns-sign">
            <span className="mns-sign-name">MapnStreets</span>
          </span>
        </header>
        <div className="mns-alert">{error}</div>
      </div>
    );
  }

  // ---- Landing page ----
  if (!hasSearched) {
    return (
      <div className="mns-page mns-page-landing">
        <div className="mns-landing-bg" />
        <div className="mns-landing">
          <span className="mns-sign mns-sign-hero">
            <span className="mns-sign-name">MapnStreets</span>
          </span>
          <h1 className="mns-landing-title">Every street name in America, mapped.</h1>
          <p className="mns-landing-sub">
            Search any street name to see where it appears, how common it is,
            and how it ranks. Use <code>*</code> for prefix matching.
          </p>
          <div className="mns-landing-search">
            <input
              className="mns-landing-input"
              value={draft}
              placeholder="Search a street name..."
              onChange={(e) => setDraft(e.target.value)}
              onKeyDown={(e) => { if (e.key === "Enter") search(); }}
              autoFocus
            />
            <button type="button" className="mns-landing-btn" onClick={() => search()}>
              Search
            </button>
          </div>
          <div className="mns-landing-chips">
            {EXAMPLES.map((ex) => (
              <button key={ex} type="button" className="mns-chip" onClick={() => search(ex)}>
                {ex}
              </button>
            ))}
          </div>
          {teaser && <p className="mns-teaser">{teaser}</p>}
        </div>
      </div>
    );
  }

  // ---- Results view ----
  return (
    <div className="mns-page">
      <header className="mns-header">
        <span className="mns-sign">
          <span className="mns-sign-name">MapnStreets</span>
        </span>
        <div className="mns-header-search">
          <input
            className="mns-header-input"
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter") search(); }}
            onBlur={commitIfChanged}
          />
          <button type="button" className="mns-header-btn" onClick={() => search()}>
            Search
          </button>
        </div>
        <span className="mns-header-count">{countMessage}</span>
      </header>

      <main className="mns-main">
        <div className="mns-ledger">
          <LedgerCell label="Matching records" value={kpis.records} caption={kpis.records_caption} />
          <LedgerCell label="Rarity rank" value={kpis.rank} caption={kpis.rank_caption} />
          <LedgerCell label="Mapped length" value={kpis.length} caption={kpis.length_caption} />
          <LedgerCell label="States present" value={kpis.states} caption={kpis.states_caption} />
        </div>
        <div className="mns-tabs-area">
          {model.get_child("content")}
        </div>
      </main>
    </div>
  );
}
