from __future__ import annotations

import streamlit as st

from job_matcher.search import search_jobs_for_cv

SOURCE_LABELS = {
    "linkedin": "LinkedIn",
    "etat_geneve": "État de Genève",
}
SOURCE_ICONS = {
    "linkedin": "app/static/source-icons/linkedin.png",
    "etat_geneve": "app/static/source-icons/etat-geneve.png",
}


def clean_company_name(company: str | None) -> str:
    company = (company or "").strip()
    if not company:
        return "Unknown company"

    cutoff = min(
        (company.index(separator) for separator in (",", "(", ":") if separator in company),
        default=len(company),
    )
    return company[:cutoff].strip() or "Unknown company"


st.set_page_config(page_title="CV Job Matcher", page_icon=":briefcase:", layout="wide")

st.title("CV Job Matcher")
st.caption("Upload a CV PDF, pick a time window, and retrieve the closest job offers from the database.")

uploaded_file = st.file_uploader("CV PDF", type=["pdf"])
filters_col, limit_col = st.columns(2)
with filters_col:
    lookback_hours = st.slider("Lookback window (hours)", min_value=1, max_value=168, value=24)
with limit_col:
    result_limit = st.segmented_control(
        "Matching offers",
        options=[25, 50, 100, 200],
        default=25,
        selection_mode="single",
    )

if st.button("Find matching offers", type="primary", use_container_width=True):
    if uploaded_file is None:
        st.error("Upload a PDF before starting the search.")
    else:
        with st.spinner("Computing CV embeddings and querying the database..."):
            try:
                cv_text, cv_chunks, results = search_jobs_for_cv(
                    uploaded_file.getvalue(),
                    lookback_hours=lookback_hours,
                    result_limit=result_limit,
                )
            except Exception as exc:
                st.exception(exc)
            else:
                left, middle, right = st.columns(3)
                left.metric("CV chars", len(cv_text))
                middle.metric("CV chunks", len(cv_chunks))
                right.metric("Matching offers", len(results))

                if not results:
                    st.warning("No offers matched the selected time window.")
                else:
                    for index, result in enumerate(results, start=1):
                        source_label = SOURCE_LABELS.get(
                            result.source or "",
                            result.source or "Source inconnue",
                        )
                        source_icon = SOURCE_ICONS.get(result.source or "")
                        source_prefix = (
                            f"![{source_label}]({source_icon}) "
                            if source_icon
                            else ""
                        )
                        company_name = clean_company_name(result.company)
                        with st.expander(
                            (
                                f"{index}. {source_prefix}"
                                f"{result.title or 'Untitled'} - "
                                f"{company_name}"
                            ),
                            expanded=index <= 3,
                        ):
                            st.markdown(
                                f"""
                                **Location:** {result.location or "N/A"}  
                                **Employment type:** {result.employment_type or "N/A"}  
                                **Industry:** {result.industry or "N/A"}  
                                **Posted at:** {result.date_posted or "N/A"}  
                                **URL:** {result.canonical_url}
                                """
                            )
                            score_a, score_b, score_c, score_d = st.columns(4)
                            score_a.metric("Title score", f"{result.title_score:.4f}")
                            score_b.metric("Text score final", f"{result.score_final:.4f}")
                            score_c.metric("Text score max", f"{result.score_max:.4f}")
                            score_d.metric("Text score top5", f"{result.score_top5_mean:.4f}")
                            left_col, right_col = st.columns(2)
                            with left_col:
                                st.markdown("**Top matching paragraph**")
                                if result.top_paragraph:
                                    st.markdown(result.top_paragraph, unsafe_allow_html=True)
                                else:
                                    st.write("N/A")
                            with right_col:
                                st.markdown("**Best CV chunk**")
                                st.write(result.top_cv_chunk or "N/A")
