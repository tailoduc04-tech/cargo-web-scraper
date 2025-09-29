import os
import pandas as pd

def save_results(scraper_data, output_filenames):
    """
    Tổng hợp và lưu kết quả scraping của một scraper cụ thể vào các file CSV.

    Args:
        scraper_data (dict): Dict chứa các list của DataFrame.
                             Ví dụ: {'main_results': [df1, df2], 'bl_summaries': [df3]}
        output_filenames (dict): Dict map từ key của scraper_data sang tên file.
                                 Ví dụ: {'main_results': 'results.csv', 'bl_summaries': 'summaries.csv'}
    """
    generated_files = []
    if not os.path.exists("output"):
        os.mkdir("output")
        
    # Lặp qua các loại dữ liệu mà scraper đã trả về (ví dụ: main_results, bl_summaries)
    for data_key, df_list in scraper_data.items():
        if not df_list:
            continue

        # Lấy tên file tương ứng từ config
        filename = os.path.join("output", output_filenames.get(data_key))
        if not filename:
            print(f"  Warning: No output filename defined for data key '{data_key}'. Skipping.")
            continue

        try:
            # Gộp tất cả các DataFrame trong danh sách thành một
            final_df = pd.concat(df_list, ignore_index=True)
            
            # Xóa các dòng trùng lặp
            final_df.drop_duplicates(inplace=True)
            
            # Lưu ra file CSV
            final_df.to_csv(filename, index=False)
            generated_files.append(filename)
            print(f"  Successfully saved data to '{filename}'")

        except Exception as e:
            print(f"  Error saving data for key '{data_key}' to '{filename}': {e}")
            
    return generated_files