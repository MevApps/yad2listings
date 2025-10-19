import os
import sys
import argparse
from pathlib import Path
import pandas as pd
from datetime import datetime

# Import the scraper modules
from scraper import VehicleScraper
import yad2_parser

# For web visualization
import dash
from dash import dcc, html, Input, Output, State, callback_context
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from dash.exceptions import PreventUpdate

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Vehicle Price Analyzer')
    parser.add_argument('--output-dir', type=str, default='scraped_vehicles',
                        help='Directory to save scraped data')
    parser.add_argument('--manufacturer', type=int, default=19,
                        help='Manufacturer ID to scrape')
    parser.add_argument('--model', type=int, default=12894,
                        help='Model ID to scrape')
    parser.add_argument('--max-pages', type=int, default=25,
                        help='Maximum number of pages to scrape')
    parser.add_argument('--skip-scrape', action='store_true',
                        help='Skip scraping and use existing data')
    parser.add_argument('--port', type=int, default=8050,
                        help='Port to run the web server on')
    return parser.parse_args()

def scrape_data(output_dir, manufacturer, model, max_pages):
    """Run the scraper to collect vehicle data"""
    print(f"Scraping data for manufacturer={manufacturer}, model={model}...")
    scraper = VehicleScraper(output_dir, manufacturer, model)
    scraper.scrape_pages(max_page=max_pages)
    
def process_data(output_dir):
    """Process the scraped HTML files into a CSV"""
    print("Processing scraped HTML files...")
    dir_name = Path(output_dir).name
    yad2_parser.process_directory(output_dir)
    output_file = f"{dir_name}_summary.csv"
    output_path = os.path.join(output_dir, output_file)
    
    # Check if the CSV file exists
    if not os.path.exists(output_path):
        print(f"Error: Could not find processed data at {output_path}")
        sys.exit(1)
        
    return output_path

def load_data(csv_path):
    """Load and prepare the CSV data for visualization"""
    try:
        df = pd.read_csv(csv_path)
        
        # Filter out cars with no price or price = 0
        df = df[df['price'] > 0]
        
        # Convert date strings to datetime objects
        df['productionDate'] = pd.to_datetime(df['productionDate'])
        
        # Extract year from production date for easier filtering
        df['productionYear'] = df['productionDate'].dt.year
        
        return df
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        sys.exit(1)

def create_dashboard(df, port=8050):
    """Create and run an interactive Dash app for visualizing the data"""
    # Create a custom stylesheet
    external_stylesheets = [
        {
            'href': 'https://fonts.googleapis.com/css2?family=Roboto:wght@400;700&display=swap',
            'rel': 'stylesheet'
        }
    ]
    # Create the app
    app = dash.Dash(
        __name__, 
        title="Vehicle Price Analyzer",
        external_stylesheets=external_stylesheets,
        suppress_callback_exceptions=True  # Needed for clientside callbacks
    )
    
    # Get unique values for filters
    km_ranges = [
        {'label': 'All', 'value': 'all'},
        {'label': '≤ 10,000 km/year', 'value': '0-10000'},
        {'label': '≤ 15,000 km/year', 'value': '0-15000'},
        {'label': '≤ 20,000 km/year', 'value': '0-20000'},
        {'label': '≤ 25,000 km/year', 'value': '0-25000'},
        {'label': '> 25,000 km/year', 'value': '25000-999999'}
    ]
    
    hands = [{'label': 'All Hands', 'value': 'all'}] + [
        {'label': f'Hand ≤ {h}', 'value': f'0-{h}'} for h in sorted(df['hand'].unique()) if h > 0
    ]
    
    sub_models = [{'label': 'All Sub-models', 'value': 'all'}] + [
        {'label': sm, 'value': sm} for sm in sorted(df['subModel'].unique())
    ]
    
    # Create model filter options
    models = [{'label': m, 'value': m} for m in sorted(df['model'].unique())]
    
    ad_types = [{'label': 'All', 'value': 'all'}] + [
        {'label': at, 'value': at} for at in sorted(df['listingType'].unique())
    ]
    
    # Define CSS styles
    styles = {
        'container': {
            'font-family': 'Roboto, sans-serif',
            'max-width': '1200px',
            'margin': '0 auto',
            'padding': '20px',
            'background-color': '#f9f9f9',
            'border-radius': '8px',
            'box-shadow': '0 4px 8px rgba(0,0,0,0.1)'
        },
        'header': {
            'background-color': '#2c3e50',
            'color': 'white',
            'padding': '15px 20px',
            'margin-bottom': '20px',
            'border-radius': '5px',
            'text-align': 'center'
        },
        'filter_container': {
            'display': 'flex',
            'flex-wrap': 'wrap',
            'gap': '15px',
            'background-color': 'white',
            'padding': '15px',
            'border-radius': '5px',
            'box-shadow': '0 2px 4px rgba(0,0,0,0.05)',
            'margin-bottom': '20px'
        },
        'filter': {
            'width': '23%',
            'min-width': '200px',
            'padding': '10px'
        },
        'label': {
            'font-weight': 'bold',
            'margin-bottom': '5px',
            'color': '#2c3e50'
        },
        'graph': {
            'background-color': 'white',
            'padding': '15px',
            'border-radius': '5px',
            'box-shadow': '0 2px 4px rgba(0,0,0,0.05)',
            'margin-bottom': '20px'
        },
        'summary': {
            'background-color': 'white',
            'padding': '15px',
            'border-radius': '5px',
            'box-shadow': '0 2px 4px rgba(0,0,0,0.05)'
        },
        'summary_header': {
            'color': '#2c3e50',
            'border-bottom': '2px solid #3498db',
            'padding-bottom': '10px',
            'margin-bottom': '15px'
        },
        'button': {
            'background-color': '#2c3e50',
            'color': 'white',
            'border': 'none',
            'padding': '10px 20px',
            'border-radius': '5px',
            'cursor': 'pointer',
            'font-weight': 'bold',
            'margin-top': '10px',
            'width': '100%'
        },
        'clear_button': {
            'background-color': '#e74c3c',
            'color': 'white',
            'border': 'none',
            'padding': '10px 20px',
            'border-radius': '5px',
            'cursor': 'pointer',
            'font-weight': 'bold',
            'margin-top': '10px',
            'width': '100%'
        },
        'click_instruction': {
            'text-align': 'center',
            'font-style': 'italic',
            'color': '#3498db',
            'margin': '10px 0',
            'padding': '8px',
            'background-color': '#f0f7ff',
            'border-radius': '5px',
            'border-left': '3px solid #3498db'
        }
    }
    
    # Create the app layout
    app.layout = html.Div([
        # Header
        html.Div([
            html.H1("Vehicle Price Analysis Dashboard", style={'margin': '0'})
        ], style=styles['header']),
        
        # Filter section
        html.Div([
            html.Div([
                html.Label("Filter by km/year:", style=styles['label']),
                dcc.Dropdown(
                    id='km-filter',
                    options=km_ranges,
                    value='all',
                    clearable=False
                ),
            ], style=styles['filter']),
            
            html.Div([
                html.Label("Filter by owner hand:", style=styles['label']),
                dcc.Dropdown(
                    id='hand-filter',
                    options=hands,
                    value='all',
                    clearable=False
                ),
            ], style=styles['filter']),
            
            # New model multi-select dropdown
            html.Div([
                html.Label("Filter by model:", style=styles['label']),
                dcc.Dropdown(
                    id='model-filter',
                    options=models,
                    value=[],
                    multi=True,
                    placeholder="Select model(s)"
                ),
            ], style=styles['filter']),
            
            html.Div([
                html.Label("Filter by listing type:", style=styles['label']),
                dcc.Dropdown(
                    id='adtype-filter',
                    options=ad_types,
                    value='all',
                    clearable=False
                ),
            ], style=styles['filter']),

            html.Div([
                html.Label("Filter by sub-model:", style=styles['label']),
                html.Div([
                    dcc.Checklist(
                        id='submodel-checklist',
                        options=[],  # Will be populated dynamically based on model selection
                        value=[],
                        labelStyle={'display': 'block', 'margin-bottom': '8px', 'cursor': 'pointer'},
                        style={'max-height': '200px', 'overflow-y': 'auto', 'padding': '10px', 'background-color': '#f5f9ff', 'border-radius': '5px'}
                    ),
                ]),
                html.Div([
                    html.Button(
                        'Apply Filters', 
                        id='apply-submodel-button', 
                        style=styles['button']
                    ),
                    html.Button(
                        'Clear Selection', 
                        id='clear-submodel-button', 
                        style=styles['clear_button']
                    ),
                ], style={'display': 'flex', 'gap': '10px'}),
            ], style={'width': '23%', 'min-width': '200px', 'padding': '10px', 'flex-grow': '1'}),
            
        ], style=styles['filter_container']),
        
        # Click instruction
        html.Div([
            html.P("👆 Click on any point in the graph to open the vehicle ad in a new tab")
        ], style=styles['click_instruction']),
        
        # Graph section
        html.Div([
            dcc.Graph(id='price-date-scatter')
        ], style=styles['graph']),
        
        # Summary section
        html.Div([
            html.H3("Data Summary", style=styles['summary_header']),
            html.Div(id='summary-stats')
        ], style=styles['summary']),
        
        # Store for clicked links - ADDED FOR CLICKABLE POINTS
        dcc.Store(id='clicked-link', storage_type='memory'),
    ], style=styles['container'])
    
    # ADDED: Client-side callback to open links in new tab
    app.clientside_callback(
        """
        function(clickData) {
            console.log(clickData);
            if(clickData && clickData.points && clickData.points.length > 0) {
                const link = clickData.points[0].customdata[6];
                if(link && link.length > 0) {
                    window.open(link, '_blank');
                }
            }
            return window.dash_clientside.no_update;
        }
        """,
        Output('clicked-link', 'data'),
        Input('price-date-scatter', 'clickData'),
        prevent_initial_call=True
    )
    
    # Callback to update submodel options based on selected models
    @app.callback(
        Output('submodel-checklist', 'options'),
        Input('model-filter', 'value'),
    )
    def update_submodel_options(selected_models):
        if not selected_models or len(selected_models) == 0:
            # If no models selected, show all submodels
            # For each submodel, add the model name in brackets
            submodel_options = []
            for sm in sorted(df['subModel'].unique()):
                # Find models for this submodel
                models_for_submodel = df[df['subModel'] == sm]['model'].unique()
                if len(models_for_submodel) == 1:
                    # If there's only one model for this submodel
                    label = f"[{models_for_submodel[0]}] {sm}"
                else:
                    # If there are multiple models, show first one with "+"
                    label = f"[{models_for_submodel[0]}+] {sm}"
                submodel_options.append({'label': label, 'value': sm})
        else:
            # Filter submodels based on selected models
            filtered_df = df[df['model'].isin(selected_models)]
            submodel_options = []
            for sm in sorted(filtered_df['subModel'].unique()):
                # Find models for this submodel (limited to selected models)
                models_for_submodel = filtered_df[filtered_df['subModel'] == sm]['model'].unique()
                if len(models_for_submodel) == 1:
                    # If there's only one model for this submodel
                    label = f" {sm} [{models_for_submodel[0]}]"
                else:
                    # Join all models (should be less since we're filtering)
                    models_str = '+'.join(models_for_submodel)
                    label = f" {sm} [{models_str}]"
                submodel_options.append({'label': label, 'value': sm})
        
        return list(sorted(submodel_options, key=lambda x: x['label']))
    
    # Callback to clear submodel selection
    @app.callback(
        Output('submodel-checklist', 'value'),
        Input('clear-submodel-button', 'n_clicks'),
        prevent_initial_call=True
    )
    def clear_submodel_selection(n_clicks):
        return []
    
    @app.callback(
        [Output('price-date-scatter', 'figure'),
         Output('summary-stats', 'children')],
        [Input('km-filter', 'value'),
         Input('hand-filter', 'value'),
         Input('model-filter', 'value'),
         Input('apply-submodel-button', 'n_clicks'),
         Input('adtype-filter', 'value')],
        [State('submodel-checklist', 'value')]
    )
    def update_graph(km_range, hand, models, submodel_btn_clicks, adtype, submodel_list):
        # Apply filters
        filtered_df = df.copy()
        
        if km_range != 'all':
            min_km, max_km = map(int, km_range.split('-'))
            filtered_df = filtered_df[filtered_df['km_per_year'] <= max_km]
            if min_km > 0:  # For the "> 25,000" filter
                filtered_df = filtered_df[filtered_df['km_per_year'] > min_km]
        
        if hand != 'all':
            # Parse the hand range format (e.g., "0-2" means hand ≤ 2)
            min_hand, max_hand = map(int, hand.split('-'))
            filtered_df = filtered_df[filtered_df['hand'] <= max_hand]
        
        # Handle model multiselect filter
        if models and len(models) > 0:
            filtered_df = filtered_df[filtered_df['model'].isin(models)]
            
        # Handle checkbox list for submodels
        if submodel_list and len(submodel_list) > 0:
            # If checkboxes are selected, filter to only those submodels
            filtered_df = filtered_df[filtered_df['subModel'].isin(submodel_list)]
        # When no checkboxes are selected, show all submodels
            
        if adtype != 'all':
            filtered_df = filtered_df[filtered_df['listingType'] == adtype]
        
        # For car price analysis, we want newest cars on the left and oldest on the right
        # First, calculate "days since newest car" for each point
        newest_date = filtered_df['productionDate'].max()
        filtered_df['days_since_newest'] = (newest_date - filtered_df['productionDate']).dt.days
        
        # Calculate actual dates instead of days since newest
        today = pd.Timestamp.today().normalize()  # Get today's date (without time)
        filtered_df['display_date'] = today - pd.to_timedelta(filtered_df['days_since_newest'], unit='D')
        
        # Create scatter plot with actual dates on x-axis
        fig = px.scatter(
            filtered_df, 
            x='display_date', 
            y='price',
            color='km_per_year',
            # Use fixed size instead of varying by km_per_year
            size_max=8,  # Slightly larger for better clickability
            color_continuous_scale='viridis',  # Smooth color gradient
            range_color=[0, filtered_df['km_per_year'].quantile(0.95)],  # Cap color scale for better differentiation
            hover_data=['model', 'subModel', 'hand', 'km', 'city', 'productionDate', 'link'],
            labels={'display_date': 'Date', 
                   'price': 'Price (₪)', 
                   'km_per_year': 'Kilometers per Year'},
            title=f'Vehicle Prices by Age ({len(filtered_df)} vehicles)'
        )
        
        # Create custom data array for hover and click functionality
        custom_data = np.column_stack((
            filtered_df['model'], 
            filtered_df['subModel'], 
            filtered_df['hand'], 
            filtered_df['km'], 
            filtered_df['city'],
            filtered_df['productionDate'],
            filtered_df['link']
        ))
        
        # UPDATED: Make points clickable to their ad links with improved styling
        fig.update_traces(
            marker=dict(
                size=8,  # Larger points for easier clicking
                opacity=0.8,
                line=dict(width=1, color='DarkSlateGrey')  # Add outline for better visibility
            ),
            customdata=custom_data,
            hovertemplate='<b>%{customdata[0]} %{customdata[1]}</b><br>' +
                          'Price: ₪%{y:,.0f}<br>' +
                          'Production Date: %{customdata[5]}<br>' +
                          'Hand: %{customdata[2]}<br>' +
                          'KM: %{customdata[3]:,.0f}<br>' +
                          'City: %{customdata[4]}<br>' +
                          '<b>👆 Click to view ad</b>'  # Clear instruction in hover
        )
        
        # UPDATED: Configure clickable points with improved settings
        fig.update_layout(
            clickmode='event+select',  # Enable clicking on points
            hoverdistance=100,         # Increase hover detection distance
            hovermode='closest',       # Show hover info for closest point
            # Improve interactivity
            dragmode='zoom',
            # Enhance appearance
            plot_bgcolor='rgba(240,240,240,0.2)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(family="Roboto, sans-serif"),
            xaxis=dict(
                title_font=dict(size=14),
                tickfont=dict(size=12),
                gridcolor='#eee',
                # Reverse the x-axis so older dates are on the left
                autorange="reversed"
            ),
            yaxis=dict(
                title_font=dict(size=14),
                tickfont=dict(size=12),
                gridcolor='#eee'
            ),
            title=dict(
                font=dict(size=16)
            ),
            legend=dict(
                title_font=dict(size=13),
                font=dict(size=11)
            ),
            coloraxis_colorbar=dict(
                title="Km/Year",
                title_font=dict(size=13),
                tickfont=dict(size=11)
            ),
            margin=dict(l=40, r=40, t=60, b=40)
        )
        
        # Always add exponential trendline
        # For car price depreciation, we'll use days since newest car as x-axis
        if len(filtered_df) > 1:
            # Sort by days_since_newest for proper fitting
            sorted_df = filtered_df.sort_values('days_since_newest')
            
            x = sorted_df['days_since_newest'].values
            y = sorted_df['price'].values
            
            # Ensure we have numeric data for curve fitting
            valid_indices = ~np.isnan(x) & ~np.isnan(y)
            x = x[valid_indices]
            y = y[valid_indices]
            
            if len(x) > 1:  # Need at least 2 points for curve fitting
                try:
                    # For better exponential fit, try more robust approaches
                    from scipy import optimize
                    
                    # For car price depreciation, an exponential decay function:
                    # Price(t) = Base_Price * exp(-decay_rate * t) + Residual_Value
                    def exp_decay_with_offset(x, a, b, c):
                        return a * np.exp(-b * x) + c
                    
                    # Initial parameter guesses with bounds
                    max_price = np.max(y)
                    mean_price = np.mean(y)
                    min_price = np.min(y)
                    
                    # Initial guess: start at max price, decay to around min price
                    p0 = [max_price - min_price, 0.001, min_price]
                    
                    # Set bounds to ensure reasonable parameters
                    # a: positive value up to 2x max observed price
                    # b: positive decay rate, not too small or large
                    # c: residual value, could be 0 or positive value
                    bounds = ([0, 0.0001, 0], [2 * max_price, 0.1, mean_price])
                    
                    # Try different fitting methods and functions
                    try:
                        # First try the 3-parameter model (with residual value)
                        params, _ = optimize.curve_fit(
                            exp_decay_with_offset, x, y, 
                            p0=p0, bounds=bounds, 
                            method='trf', maxfev=10000
                        )
                        a, b, c = params
                        
                        # Generate curve points with more granularity
                        x_curve = np.linspace(0, x.max(), 200)
                        y_curve = exp_decay_with_offset(x_curve, a, b, c)
                        
                    except RuntimeError:
                        # If that fails, try simpler 2-parameter model without offset
                        def exp_decay(x, a, b):
                            return a * np.exp(-b * x)
                        
                        # Adjust bounds for simpler model
                        p0_simple = [max_price, 0.001]
                        bounds_simple = ([0, 0.0001], [2 * max_price, 0.1])
                        
                        params, _ = optimize.curve_fit(
                            exp_decay, x, y, 
                            p0=p0_simple, bounds=bounds_simple, 
                            method='trf', maxfev=10000
                        )
                        a, b = params
                        c = 0  # No offset
                        
                        # Generate curve points
                        x_curve = np.linspace(0, x.max(), 200)
                        y_curve = exp_decay(x_curve, a, b)
                    
                    # Convert x_curve back to days for plotting
                    curve_days = x_curve
                    
                    # Find newest date for reference
                    newest_date = filtered_df['productionDate'].max()
                    
                    # Convert days to dates for plotting
                    curve_dates = [newest_date - pd.Timedelta(days=int(days)) for days in curve_days]
                    
                    # Add the exponential trendline with higher visibility
                    fig.add_trace(go.Scatter(
                        x=today - pd.to_timedelta(curve_days, unit='D'),  # Convert to actual dates
                        y=y_curve,
                        mode='lines',
                        name='Exponential Trend',
                        line=dict(color='red', width=3, dash='solid'),
                        hoverinfo='none'  # Disable hover for the trendline to keep it clean
                    ))
                    
                except Exception as e:
                    # Log the error for debugging
                    print(f"Error fitting exponential curve: {str(e)}")
                    
                    # Fallback to simple exponential fit using numpy
                    try:
                        # Take log of y values for linear fit
                        log_y = np.log(y)
                        # Filter out any -inf values from log(0)
                        valid = np.isfinite(log_y)
                        x_valid = x[valid]
                        log_y_valid = log_y[valid]
                        
                        if len(x_valid) > 1:
                            # Linear fit on log-transformed data
                            z = np.polyfit(x_valid, log_y_valid, 1)
                            # Convert back to exponential form
                            a = np.exp(z[1])
                            b = -z[0]  # Negative because our formula is exp(-bx)
                            
                            # Generate curve points
                            x_curve = np.linspace(0, x.max(), 200)
                            y_curve = a * np.exp(-b * x_curve)
                            
                            # Add the exponential trendline
                            fig.add_trace(go.Scatter(
                                x=today - pd.to_timedelta(x_curve, unit='D'),  # Convert to actual dates
                                y=y_curve,
                                mode='lines',
                                name='Exponential Trend (Simplified)',
                                line=dict(color='red', width=3, dash='solid'),
                                hoverinfo='none'
                            ))
                        else:
                            # Not enough valid data for simplified exponential fit
                            # Fall back to linear as last resort
                            z = np.polyfit(x, y, 1)
                            p = np.poly1d(z)
                            x_curve = np.linspace(0, x.max(), 200)
                            
                            fig.add_trace(go.Scatter(
                                x=today - pd.to_timedelta(x_curve, unit='D'),  # Convert to actual dates
                                y=p(x_curve),
                                mode='lines',
                                name='Linear Trend (Fallback)',
                                line=dict(color='orange', width=3, dash='dash'),
                                hoverinfo='none'
                            ))
                            
                    except Exception as e2:
                        print(f"Error with simplified exponential fit: {str(e2)}")
                        # Final fallback to linear trend if all else fails
                        try:
                            z = np.polyfit(x, y, 1)
                            p = np.poly1d(z)
                            x_curve = np.linspace(0, x.max(), 200)
                            
                            fig.add_trace(go.Scatter(
                                x=today - pd.to_timedelta(x_curve, unit='D'),  # Convert to actual dates
                                y=p(x_curve),
                                mode='lines',
                                name='Linear Trend (Fallback)',
                                line=dict(color='orange', width=3, dash='dash'),
                                hoverinfo='none'
                            ))
                        except:
                            print("All trendline methods failed")
        
        # Enhanced summary statistics with better styling
        summary_style = {
            'container': {
                'display': 'flex',
                'flex-wrap': 'wrap',
                'gap': '20px'
            },
            'card': {
                'flex': '1',
                'min-width': '180px',
                'padding': '15px',
                'border-radius': '5px',
                'background-color': '#f5f9ff',
                'box-shadow': '0 2px 4px rgba(0,0,0,0.05)',
                'text-align': 'center'
            },
            'value': {
                'font-size': '20px',
                'font-weight': 'bold',
                'color': '#2c3e50',
                'margin': '10px 0'
            },
            'label': {
                'font-size': '14px',
                'color': '#7f8c8d',
                'margin': '0'
            }
        }
        
        # Create styled summary stats cards
        summary = html.Div([
            html.Div([
                html.P("Number of Vehicles", style=summary_style['label']),
                html.P(f"{len(filtered_df)}", style=summary_style['value'])
            ], style=summary_style['card']),
            
            html.Div([
                html.P("Average Price", style=summary_style['label']),
                html.P(f"₪{filtered_df['price'].mean():,.0f}", style=summary_style['value'])
            ], style=summary_style['card']),
            
            html.Div([
                html.P("Price Range", style=summary_style['label']),
                html.P(f"₪{filtered_df['price'].min():,.0f} - ₪{filtered_df['price'].max():,.0f}", style=summary_style['value'])
            ], style=summary_style['card']),
            
            html.Div([
                html.P("Average km/year", style=summary_style['label']),
                html.P(f"{filtered_df['km_per_year'].mean():,.0f}", style=summary_style['value'])
            ], style=summary_style['card']),
            
            html.Div([
                html.P("Average Vehicle Age", style=summary_style['label']),
                html.P(f"{filtered_df['number_of_years'].mean():.1f} years", style=summary_style['value'])
            ], style=summary_style['card']),
        ], style=summary_style['container'])
        
        return fig, summary
    
    # Run the app
    print(f"Starting dashboard on http://127.0.0.1:{port}/")
    app.run(debug=False, port=port)


def main():
    args = parse_arguments()
    
    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Step 1: Scrape the data if not skipped
    if not args.skip_scrape:
        scrape_data(args.output_dir, args.manufacturer, args.model, args.max_pages)
    
    # Step 2: Process the scraped data
    csv_path = process_data(args.output_dir)
    
    # Step 3: Load the data
    df = load_data(csv_path)
    os.unlink(csv_path)
    
    # Step 4: Create and run the dashboard
    create_dashboard(df, args.port)


if __name__ == "__main__":
    main()