% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils-model.R
\docType{class}
\name{Model}
\alias{Model}
\title{Create the definition of multi-table and single-table datasets}
\description{
Create the definition of a dataset containing one or more tables. The definition includes the name and description of the dataset and the name and description of each table, attribute, and metric within the dataset.
}
\examples{
\donttest{
# Create data frames
df1 <- data.frame("id" = c(1, 2, 3, 4, 5),
                  "first_name" = c("Jason", "Molly", "Tina", "Jake", "Amy"),
                  "last_name" = c("Miller", "Jacobson", "Turner", "Milner", "Cooze"))

df2 <- data.frame("id" = c(1, 2, 3, 4, 5),
                  "age" = c(42, 52, 36, 24, 73),
                  "state" = c("VA", "NC", "WY", "CA", "CA"),
                  "salary" = c(50000, 100000, 75000, 85000, 250000))

# Create a list of tables containing one or more tables and their names
tables = list(list("table_name" = "employee_id",
                    "data_frame" = df1),
              list("table_name" = "employee_data",
                   "data_frame" = df2))

# Generate the data model
model <- Model$new(tables=tables, name="Employees", description="Employee Analytics Data")
model_info <- model$get_model()
}
}
\keyword{internal}
\section{Public fields}{
\if{html}{\out{<div class="r6-fields">}}
\describe{
\item{\code{tables}}{List containing lists of data.frames and corresponding table names}

\item{\code{name}}{Name of the dataset}

\item{\code{description}}{Description of the data set. Must be less than or equal to 250 characters}

\item{\code{folder_id}}{ID of the shared folder that the dataset should be created within. If NULL, defaults to the user's My Reports folder}
}
\if{html}{\out{</div>}}
}
\section{Methods}{
\subsection{Public methods}{
\itemize{
\item \href{#method-new}{\code{Model$new()}}
\item \href{#method-get_model}{\code{Model$get_model()}}
\item \href{#method-get_name}{\code{Model$get_name()}}
\item \href{#method-get_description}{\code{Model$get_description()}}
\item \href{#method-get_folder_id}{\code{Model$get_folder_id()}}
\item \href{#method-get_tables}{\code{Model$get_tables()}}
\item \href{#method-get_attributes}{\code{Model$get_attributes()}}
\item \href{#method-get_metrics}{\code{Model$get_metrics()}}
\item \href{#method-clone}{\code{Model$clone()}}
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-new"></a>}}
\if{latex}{\out{\hypertarget{method-new}{}}}
\subsection{Method \code{new()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Model$new(tables, name, description = NULL, folder_id = NULL)}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-get_model"></a>}}
\if{latex}{\out{\hypertarget{method-get_model}{}}}
\subsection{Method \code{get_model()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Model$get_model()}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-get_name"></a>}}
\if{latex}{\out{\hypertarget{method-get_name}{}}}
\subsection{Method \code{get_name()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Model$get_name()}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-get_description"></a>}}
\if{latex}{\out{\hypertarget{method-get_description}{}}}
\subsection{Method \code{get_description()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Model$get_description()}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-get_folder_id"></a>}}
\if{latex}{\out{\hypertarget{method-get_folder_id}{}}}
\subsection{Method \code{get_folder_id()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Model$get_folder_id()}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-get_tables"></a>}}
\if{latex}{\out{\hypertarget{method-get_tables}{}}}
\subsection{Method \code{get_tables()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Model$get_tables()}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-get_attributes"></a>}}
\if{latex}{\out{\hypertarget{method-get_attributes}{}}}
\subsection{Method \code{get_attributes()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Model$get_attributes()}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-get_metrics"></a>}}
\if{latex}{\out{\hypertarget{method-get_metrics}{}}}
\subsection{Method \code{get_metrics()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Model$get_metrics()}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-clone"></a>}}
\if{latex}{\out{\hypertarget{method-clone}{}}}
\subsection{Method \code{clone()}}{
The objects of this class are cloneable with this method.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Model$clone(deep = FALSE)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{deep}}{Whether to make a deep clone.}
}
\if{html}{\out{</div>}}
}
}
}
