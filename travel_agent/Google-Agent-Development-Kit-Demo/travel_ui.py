import streamlit as st
import requests

st.set_page_config(page_title="ADK-Powered Travel Planner", page_icon="✈️")

st.title("🌍 ADK-Powered Travel Planner")

# ✨ Add start location here
origin = st.text_input("Where are you flying from?", placeholder="e.g., New York")

destination = st.text_input("Destination", placeholder="e.g., Paris")
start_date = st.date_input("Start Date")
end_date = st.date_input("End Date")
budget = st.number_input("Budget (in USD)", min_value=100, step=50)

if st.button("Plan My Trip ✨"):
    if not all([origin, destination, start_date, end_date, budget]):
        st.warning("Please fill in all the details.")
    else:
        payload = {
            "origin": origin,
            "destination": destination,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "budget": budget
        }
        response = requests.post("http://localhost:8000/run", json=payload)

        if response.ok:
            data = response.json()
            st.subheader("✈️ Flights")
            st.markdown(data["flights"])
            st.subheader("🏨 Stays")
            st.markdown(data["stay"])
            st.subheader("🗺️ Activities")
            st.markdown(data["activities"])
        else:
            st.error("Failed to fetch travel plan. Please try again.")
