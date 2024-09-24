// app/components/OlympicGamesSelector.tsx

import React from "react";
import Image from "next/image";
import { FaChevronDown, FaChevronUp } from "react-icons/fa";

interface HostCity {
  year: number;
  season: string;
  game: string;
  host_city: string;
}

interface OlympicGamesSelectorProps {
  summerGames: HostCity[];
  winterGames: HostCity[];
  selectedOlympics: string;
  setSelectedOlympics: (game: string) => void;
  isToggleOpen: boolean;
  setIsToggleOpen: (open: boolean) => void;
}

const OlympicGamesSelector: React.FC<OlympicGamesSelectorProps> = ({
  summerGames,
  winterGames,
  selectedOlympics,
  setSelectedOlympics,
  isToggleOpen,
  setIsToggleOpen,
}) => {
  // Helper function to get image sources with fallbacks
  const getImageSrc = (year: number, season: string) => {
    const basePath = `/logos/${season.toLowerCase()}/${year}`;
    return {
      src: `${basePath}.svg`,
      fallback1: `${basePath}.png`,
      fallback2: `${basePath}.jpg`,
    };
  };

  return (
    <>
      {/* Olympic Games Selection Toggle Button */}
      <div className="mb-4 mt-4 flex justify-center">
        <button
          className="flex items-center justify-center p-2 bg-transparent border-2 border-olympicBlue rounded-full focus:outline-none focus:ring-2 focus:ring-olympicBlue"
          onClick={() => setIsToggleOpen(!isToggleOpen)}
          aria-expanded={isToggleOpen}
          aria-label="Toggle Olympic Games Selection"
        >
          {isToggleOpen ? <FaChevronUp size={20} /> : <FaChevronDown size={20} />}
        </button>
      </div>

      {/* Olympic Games Selection Buttons (Summer and Winter) */}
      {isToggleOpen && (
        <div className="mb-8 mt-2 flex justify-center flex-wrap gap-4">
          {/* Summer Games */}
          <div className="flex flex-col items-center">
            <h2 className="mb-2">Summer</h2>
            <div className="flex flex-wrap gap-2">
              {summerGames.map(({ year, season, game }) => {
                const isSelected = selectedOlympics === game;
                const { src, fallback1, fallback2 } = getImageSrc(year, season);

                return (
                  <button
                    key={`${year}-${season}`}
                    className={`olympic-game-button ${isSelected ? "selected" : ""}`}
                    onClick={() => setSelectedOlympics(game)}
                    aria-pressed={isSelected}
                  >
                    <ImageWithFallback
                      src={src}
                      fallback1={fallback1}
                      fallback2={fallback2}
                      alt={`${year} ${season} Olympics`}
                    />
                  </button>
                );
              })}
            </div>
          </div>

          {/* Winter Games */}
          <div className="flex flex-col items-center">
            <h2 className="mb-2">Winter</h2>
            <div className="flex flex-wrap gap-2">
              {winterGames.map(({ year, season, game }) => {
                const isSelected = selectedOlympics === game;
                const { src, fallback1, fallback2 } = getImageSrc(year, season);

                return (
                  <button
                    key={`${year}-${season}`}
                    className={`olympic-game-button ${isSelected ? "selected" : ""}`}
                    onClick={() => setSelectedOlympics(game)}
                    aria-pressed={isSelected}
                  >
                    <ImageWithFallback
                      src={src}
                      fallback1={fallback1}
                      fallback2={fallback2}
                      alt={`${year} ${season} Olympics`}
                    />
                  </button>
                );
              })}
            </div>
          </div>
        </div>
      )}
    </>
  );
};

// Custom Image component with fallbacks
interface ImageWithFallbackProps {
  src: string;
  fallback1: string;
  fallback2: string;
  alt: string;
}

const ImageWithFallback: React.FC<ImageWithFallbackProps> = ({
  src,
  fallback1,
  fallback2,
  alt,
}) => {
  const [imgSrc, setImgSrc] = React.useState<string>(src);

  const handleError = () => {
    if (imgSrc === src) {
      setImgSrc(fallback1);
    } else if (imgSrc === fallback1) {
      setImgSrc(fallback2);
    }
  };

  return (
    <Image
      src={imgSrc}
      alt={alt}
      width={60}
      height={60}
      className="rounded-full"
      onError={handleError}
      objectFit="contain"
    />
  );
};

export default OlympicGamesSelector;