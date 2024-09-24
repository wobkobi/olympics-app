// app/components/ImageWithFallback.tsx

import React, { useState } from 'react';
import Image from 'next/image';

interface ImageWithFallbackProps {
  src: string; // Base path without extension
  alt: string;
  width: number;
  height: number;
  className?: string;
}

const ImageWithFallback: React.FC<ImageWithFallbackProps> = ({ src, alt, width, height, className }) => {
  const [currentSrc, setCurrentSrc] = useState<string>(`${src}.svg`);
  const [attempt, setAttempt] = useState<number>(0); // 0: SVG, 1: PNG, 2: JPG

  const handleError = () => {
    if (attempt === 0) {
      setCurrentSrc(`${src}.png`);
      setAttempt(1);
    } else if (attempt === 1) {
      setCurrentSrc(`${src}.jpg`);
      setAttempt(2);
    }
    // If all attempts fail, you might want to set a default placeholder image
  };

  return (
    <Image
      src={currentSrc}
      alt={alt}
      width={width}
      height={height}
      className={className}
      onError={handleError}
    />
  );
};

export default ImageWithFallback;
