/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.operator;

import com.facebook.presto.spi.block.Block;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class StandardJoinFilterFunctionVerifier
        implements JoinFilterFunctionVerifier
{
    private static final Block[] EMPTY_BLOCK_ARRAY = new Block[0];

    private final JoinFilterFunction filterFunction;
    private final List<Block[]> channelArrays;

    public StandardJoinFilterFunctionVerifier(JoinFilterFunction filterFunction, List<List<Block>> channels)
    {
        this.channelArrays = buildChannelArrays(requireNonNull(channels, "channels can not be null"));
        this.filterFunction = requireNonNull(filterFunction, "filterFunction can not be null");
    }

    private List<Block[]> buildChannelArrays(List<List<Block>> channels)
    {
        if (channels.isEmpty()) {
            return new ArrayList<>();
        }

        int pagesCount = channels.get(0).size();
        List<Block[]> channelArrays = new ArrayList<>();
        for (int i = 0; i < pagesCount; ++i) {
            Block[] blocks = new Block[channels.size()];
            for (int j = 0; j < channels.size(); ++j) {
                blocks[j] = channels.get(j).get(i);
            }
            channelArrays.add(blocks);
        }
        return channelArrays;
    }

    public boolean applyFilterFunction(int leftBlockIndex, int leftPosition, int rightPosition, Block[] allRightBlocks)
    {
        return filterFunction.filter(leftPosition, getLeftBlocks(leftBlockIndex), rightPosition, allRightBlocks);
    }

    private Block[] getLeftBlocks(int leftBlockIndex)
    {
        if (channelArrays.isEmpty()) {
            return EMPTY_BLOCK_ARRAY;
        }
        else {
            return channelArrays.get(leftBlockIndex);
        }
    }
}
