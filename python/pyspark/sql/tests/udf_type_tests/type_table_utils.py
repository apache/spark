#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import sys
import argparse
import re
from typing import List, Tuple, Optional

CELL_WIDTH = 30


class Colors:
    """ANSI color codes for terminal output"""

    RESET = "\033[0m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    BOLD = "\033[1m"
    BG_RED = "\033[101m"
    BG_GREEN = "\033[102m"


def parse_table_line(line: str) -> Optional[List[str]]:
    """Parse a table line and extract cell contents."""
    if not line.strip() or line.strip().startswith("+"):
        return None

    cells = [cell.strip() for cell in line.strip("|").split("|")]
    return cells


def parse_table_content(content: str) -> Tuple[List[str], List[List[str]]]:
    """Parse table content and return header and rows."""
    lines = content.strip().split("\n")
    header = None
    rows = []

    for line in lines:
        cells = parse_table_line(line)
        if cells is not None:
            if header is None:
                header = cells
            else:
                rows.append(cells)

    return header, rows


def highlight_cell_diff(
    expected_cell: str, actual_cell: str, use_colors: bool = True, cell_width: int = CELL_WIDTH
) -> str:
    """Highlight differences within a single cell, showing inline diff with both parts visible."""
    if expected_cell == actual_cell:
        return expected_cell

    if use_colors:
        format_overhead = 1
    else:
        format_overhead = 6

    total_content_length = len(expected_cell) + len(actual_cell) + format_overhead

    if total_content_length > cell_width:
        available_space = cell_width - format_overhead
        half_space = available_space // 2

        expected_truncated = expected_cell
        actual_truncated = actual_cell

        if len(expected_cell) > half_space:
            expected_truncated = expected_cell[: half_space - 3] + "..."
        if len(actual_cell) > half_space:
            actual_truncated = actual_cell[: half_space - 3] + "..."
    else:
        expected_truncated = expected_cell
        actual_truncated = actual_cell

    if use_colors:
        return (
            f"{Colors.BG_RED}{expected_truncated}{Colors.RESET}→"
            f"{Colors.BG_GREEN}{actual_truncated}{Colors.RESET}"
        )
    else:
        return f"[-{expected_truncated}-][+{actual_truncated}+]"


def format_table_diff(
    header: List[str],
    expected_rows: List[List[str]],
    actual_rows: List[List[str]],
    use_colors: bool = True,
    cell_width: int = CELL_WIDTH,
) -> str:
    """Format a table diff with cell-level highlighting."""
    output_lines = []

    title = "Table Comparison (Expected vs Actual)"
    output_lines.append(
        f"\n{Colors.BOLD if use_colors else ''}{title}{Colors.RESET if use_colors else ''}"
    )
    output_lines.append("=" * len(title))

    col_widths = [cell_width] * len(header)

    def format_row(cells: List[str], prefix: str = "", color: str = "") -> str:
        """Format a single row with proper alignment."""
        formatted_cells = []
        for i, (cell, width) in enumerate(zip(cells, col_widths)):
            display_cell = str(cell)

            visible_length = len(re.sub(r"\x1b\[[0-9;]*m", "", display_cell))

            if visible_length > width:
                truncated = ""
                visible_count = 0
                i = 0
                while i < len(display_cell) and visible_count < width - 3:
                    if display_cell[i : i + 1] == "\x1b":
                        end = display_cell.find("m", i)
                        if end != -1:
                            truncated += display_cell[i : end + 1]
                            i = end + 1
                        else:
                            i += 1
                    else:
                        truncated += display_cell[i]
                        visible_count += 1
                        i += 1
                display_cell = truncated + "..."
                visible_length = visible_count + 3

            padding = width - visible_length
            formatted_cells.append(display_cell + " " * padding)

        row_content = f"|{' |'.join(formatted_cells)} |"
        if color and use_colors:
            return f"{prefix}{color}{row_content}{Colors.RESET}"
        return f"{prefix}{row_content}"

    def create_border(char: str = "-") -> str:
        """Create a table border."""
        return "+" + "+".join(char * (width + 1) for width in col_widths) + "+"

    output_lines.append(create_border())
    output_lines.append(format_row(header))
    output_lines.append(create_border())

    max_rows = max(len(expected_rows), len(actual_rows))
    changes_found = False

    for row_idx in range(max_rows):
        expected_row = expected_rows[row_idx] if row_idx < len(expected_rows) else None
        actual_row = actual_rows[row_idx] if row_idx < len(actual_rows) else None

        if expected_row is None:
            display_row = actual_row[:] if actual_row else []
            while len(display_row) < len(header):
                display_row.append("")
            output_lines.append(format_row(display_row, "+ ", Colors.GREEN if use_colors else ""))
            changes_found = True
        elif actual_row is None:
            display_row = expected_row[:] if expected_row else []
            while len(display_row) < len(header):
                display_row.append("")
            output_lines.append(format_row(display_row, "- ", Colors.RED if use_colors else ""))
            changes_found = True
        else:
            row_has_changes = False
            diff_row = []

            for col_idx in range(len(header)):
                expected_cell = expected_row[col_idx] if col_idx < len(expected_row) else ""
                actual_cell = actual_row[col_idx] if col_idx < len(actual_row) else ""

                if expected_cell != actual_cell:
                    row_has_changes = True
                    diff_cell = highlight_cell_diff(
                        expected_cell, actual_cell, use_colors, cell_width
                    )
                    diff_row.append(diff_cell)
                else:
                    diff_row.append(expected_cell)

            while len(diff_row) < len(header):
                diff_row.append("")

            output_lines.append(format_row(diff_row))
            if row_has_changes:
                changes_found = True

    output_lines.append(create_border())

    if not changes_found:
        green_start = Colors.GREEN if use_colors else ""
        reset_end = Colors.RESET if use_colors else ""
        output_lines.append(f"\n{green_start}✓ Tables are identical!{reset_end}")
    else:
        legend = "\nLegend:"
        if use_colors:
            legend += f"\n  {Colors.BG_RED}Red background{Colors.RESET}: Expected content (removed)"
            legend += f"\n  {Colors.BG_GREEN}Green background{Colors.RESET}: Actual content (added)"
        else:
            legend += "\n  [-text-]: Expected content (removed)"
            legend += "\n  [+text+]: Actual content (added)"
        legend += "\n  Lines prefixed with '-': Expected only rows"
        legend += "\n  Lines prefixed with '+': Actual only rows"
        output_lines.append(legend)

    return "\n".join(output_lines)


def generate_table_diff(actual, expected, cell_width=CELL_WIDTH):
    """Generate a table-aware diff between actual and expected output."""
    try:
        expected_header, expected_rows = parse_table_content(expected)
        actual_header, actual_rows = parse_table_content(actual)

        if expected_header and actual_header:
            return format_table_diff(expected_header, expected_rows, actual_rows, True, cell_width)
    except Exception:
        pass

    return "Unable to parse content as table format."


def format_type_table(results, header, column_width=30):
    """Format results into an ASCII table with the given header and column width.

    Args:
        results: List of rows, where each row is a list of values
        header: List of header strings
        column_width: Width of each column (default: 30)

    Returns:
        String representation of the formatted table
    """
    column_widths = [column_width] * len(header)
    output_lines = []

    top_border = "+" + "+".join("-" * (width + 1) for width in column_widths) + "+"
    output_lines.append(top_border)

    header_line = (
        "|"
        + "|".join(f"{cell[:width]:<{width}} " for cell, width in zip(header, column_widths))
        + "|"
    )
    output_lines.append(header_line)
    output_lines.append(top_border)

    for row in results:
        data_line = (
            "|"
            + "|".join(f"{str(cell)[:width]:<{width}} " for cell, width in zip(row, column_widths))
            + "|"
        )
        output_lines.append(data_line)

    output_lines.append(top_border)
    return "\n".join(output_lines)


def compare_files(file1_path, file2_path, cell_width=CELL_WIDTH):
    """Compare two files and show the differences."""
    if not os.path.exists(file1_path):
        print(f"Error: File '{file1_path}' does not exist")
        return False

    if not os.path.exists(file2_path):
        print(f"Error: File '{file2_path}' does not exist")
        return False

    try:
        with open(file1_path, "r") as f1:
            content1 = f1.read()

        with open(file2_path, "r") as f2:
            content2 = f2.read()
    except Exception as e:
        print(f"Error reading files: {e}")
        return False

    print(f"Comparing '{file1_path}' (expected) with '{file2_path}' (actual)")
    print("=" * 80)

    if content1 == content2:
        print("Files are identical!")
        return True
    else:
        print("Files differ. Generating word-wise diff...")
        print()

        diff_output = generate_table_diff(content2, content1, cell_width)
        print(diff_output)
        return False


def main():
    parser = argparse.ArgumentParser(description="Compare two table files using word-wise diff")
    parser.add_argument("file1", help="First file (expected)")
    parser.add_argument("file2", help="Second file (actual)")
    parser.add_argument(
        "--cell-width",
        type=int,
        default=CELL_WIDTH,
        help=f"Width of each table cell (default: {CELL_WIDTH})",
    )

    args = parser.parse_args()

    success = compare_files(args.file1, args.file2, args.cell_width)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
