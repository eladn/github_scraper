import os
import asyncio
from typing import Dict, List
from collections import defaultdict

import aiofile
from aiopath import AsyncPath


__all__ = ['AsyncIOFileHelper']


class AsyncIOFileHelper:
    def __init__(self, max_nr_concurrent_operations: int):
        self.concurrent_operations_sem = asyncio.BoundedSemaphore(max_nr_concurrent_operations)

    async def async_copy_file(self, source_file_path: str, dest_file_path: str, chunk_size: int = 65535):
        # We avoid making too many concurrent IOs to FS.
        async with self.concurrent_operations_sem:
            async with aiofile.async_open(source_file_path, "rb") as src, \
                    aiofile.async_open(dest_file_path, "wb") as dest:
                async for chunk in src.iter_chunked(chunk_size):
                    await dest.write(chunk)

    async def recursively_find_and_copy_all_files_with_certain_extension(
            self, look_in_dir_path: str, tgt_dir_path: str, file_extensions: List[str]):
        filename_occurrences: Dict[str, int] = defaultdict(int)
        tasks = []
        # note: files might be duplicated if for two file extensions one is a prefix of the other.
        for file_ext in file_extensions:
            async for file_path in AsyncPath(look_in_dir_path).glob(f'**/*.{file_ext}'):
                if not await AsyncPath(file_path).is_file():
                    continue
                filename = os.path.basename(file_path)
                filename_occurrences[filename] += 1
                if filename_occurrences[filename] > 1:
                    # matching_extensions = tuple(ext for ext in file_extensions if filename.endswith(f'.{ext}'))
                    # assert len(matching_extensions) > 0
                    # file_extension = max(matching_extensions, key=len)
                    filename = f"{filename.rstrip(f'.{file_ext}')}__" \
                               f"{filename_occurrences[filename]}.{file_ext}"
                    assert filename not in filename_occurrences
                tasks.append(asyncio.create_task(self.async_copy_file(
                    source_file_path=file_path,
                    dest_file_path=os.path.join(tgt_dir_path, filename))))
        await asyncio.gather(*tasks)
