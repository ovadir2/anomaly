import streamlit as st
import pyarrow as pa
import pyarrow.hdfs as hdfs
def main():
    st.title("Simple Streamlit App")
    
    # Add text to the app
    st.header("Welcome to my Streamlit app!")
    st.write("This is a simple example to get you started.")
    
    # Add an interactive widget
    name = st.text_input("Enter your name:")
    if name:
        st.write(f"Hello, {name}!")
    
    # Add a plot or visualization
    st.subheader("Plot")
    data = [1, 2, 3, 4, 5]
    st.line_chart(data)
    
    # Add additional content
    st.markdown("### About")
    st.write("This app demonstrates the basic features of Streamlit.")
    st.write("Feel free to modify it and explore more!")
    
if __name__ == '__main__':
    main()
